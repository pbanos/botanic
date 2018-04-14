package redisq

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pbanos/botanic/queue"
	redis "gopkg.in/redis.v5"
)

/*
EncodeDecoder is an interface for objects
that allow encoding tasks as slices of bytes and decoding
them back to tasks. It is used to serialize tasks into a
representation to store on redis
*/
type EncodeDecoder interface {

	//Encode receives a *queue.Task
	// and returns a slice of bytes with the task encoded or an
	//error if the encoding could not be performed for
	//some reason. Its counterpart is TaskDecoder.
	Encode(context.Context, *queue.Task) ([]byte, error)

	//Decode receives a slice of bytes
	//and returns a *queue.Task decoded from the slice of bytes
	//or an error if the decoding could not be performed
	//for some reason.
	Decode(context.Context, []byte) (*queue.Task, error)
}

type redisQ struct {
	id         string
	rc         *redis.Client
	allTaskCtx context.Context
	allTaskCF  context.CancelFunc
	taskMaxRun time.Duration
	lockTTL    time.Duration
	EncodeDecoder
}

const lockReleaseScript = `
if redis.call("GET",KEYS[1]) == ARGV[1] then
    return redis.call("DEL",KEYS[1])
else
    return 0
end
`
const lockAttempts = 5
const failToLockSleep = 10 * time.Millisecond

/*
New returns a queue.Queue that uses the given redis client as a
backend. It uses the given id to prefix the keys used on the
redis client to keep the queue's data, which are the following:
  * id:pending is the key to a set with the ids of the pending tasks
  * id:running is the key to a set with the ids of the running tasks
  * id:task:task_id:data is the key to a string that holds the task data.
  Tasks are encoded and decoded using the given EncodeDecoder.
  * id:task:task_id:lock implements a lock for exclusive management of a
  task on the queue. It is set to expire in the given lockTTL duration
  * id:task:task_id:running implements a mark to set the task is already running,
  that expires in the given taskMaxRun duration. Once the key expires
  a cleanup process will understand the task was dropped by a failing
  worker. Setting it to the zero value prevents the key from expiring
  and the cleanup process from taking place at all.

The returned queue is secure for concurrent use by multiple goroutines.
*/
func New(id string, rc *redis.Client, taskMaxRun, lockTTL time.Duration, encDec EncodeDecoder) queue.Queue {
	ctx, cf := context.WithCancel(context.Background())
	rq := &redisQ{
		id:            id,
		rc:            rc,
		allTaskCtx:    ctx,
		allTaskCF:     cf,
		taskMaxRun:    taskMaxRun,
		lockTTL:       lockTTL,
		EncodeDecoder: encDec,
	}
	go rq.dropTimedOutTasks()
	return rq
}

// Push takes a task and stores it in the queue or
// returns an error. The task will count as pending.
func (rq *redisQ) Push(ctx context.Context, t *queue.Task) error {
	data, err := rq.Encode(ctx, t)
	if err != nil {
		return fmt.Errorf("pushing task %s to queue: %v", t.ID(), err)
	}
	tKeyPrefix := rq.taskKeyPrefix(t.ID())
	tDataKey := fmt.Sprintf("%s:data", tKeyPrefix)
	boolCMD := rq.rc.SetNX(tDataKey, string(data), time.Duration(0))
	ok, err := boolCMD.Result()
	if err != nil {
		return fmt.Errorf("pushing task %s to queue: %v", t.ID(), err)
	}
	if !ok {
		return fmt.Errorf("pushing task %s to queue: key %q already exists", t.ID(), tDataKey)
	}
	intCMD := rq.rc.SAdd(rq.pendingSetKey(), tKeyPrefix)
	added, err := intCMD.Result()
	if err != nil || added != 1 {
		rq.rc.Del(tDataKey)
		if err == nil {
			err = fmt.Errorf("%q already in pending set %q", tKeyPrefix, rq.pendingSetKey())
		}
		return fmt.Errorf("pushing task %s to queue %v: %v", t.ID(), rq, err)
	}
	return nil
}

// Pull returns a task and a context that may have
// a timeout or allow its cancellation, or an error.
// The pulled task will be counted as running from
// then on.
// If there are no tasks to pull, implementations
// should not return an error, but 4 nil values.
// In case of cancellation, workers should still
// drop the task.
func (rq *redisQ) Pull(ctx context.Context) (*queue.Task, context.Context, context.CancelFunc, error) {
	iter := rq.rc.SScan(rq.pendingSetKey(), 0, "", 0).Iterator()
	for iter.Next() {
		var tctx context.Context
		var tcf context.CancelFunc
		if rq.taskMaxRun == 0 {
			tctx, tcf = rq.allTaskCtx, func() {}
		} else {
			tctx, tcf = context.WithTimeout(rq.allTaskCtx, rq.taskMaxRun)
		}
		taskKeyPrefix := iter.Val()
		err := rq.withLockFor(ctx, taskKeyPrefix, 0, func(ctx context.Context) error {
			ok, err := rq.rc.SetNX(fmt.Sprintf("%s:running", taskKeyPrefix), "true", rq.taskMaxRun).Result()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("task %q already running", taskKeyPrefix)
			}
			_, err = rq.rc.SMove(rq.pendingSetKey(), rq.runningSetKey(), taskKeyPrefix).Result()
			if err != nil {
				if ctx.Err() == nil {
					rq.rc.Del(fmt.Sprintf("%s:running", taskKeyPrefix))
				}
				return fmt.Errorf("moving %q from %q set to %q set: %v", taskKeyPrefix, rq.pendingSetKey(), rq.runningSetKey(), err)
			}
			return nil
		})
		if err == nil {
			pTokens := strings.Split(taskKeyPrefix, ":")
			tData, err := rq.rc.Get(fmt.Sprintf("%s:data", taskKeyPrefix)).Result()
			if err != nil {
				tcf()
				rq.Drop(ctx, pTokens[len(pTokens)-1])
				continue
			}
			t, err := rq.Decode(ctx, []byte(tData))
			if err != nil {
				tcf()
				rq.Drop(ctx, pTokens[len(pTokens)-1])
				continue
			}

			return t, tctx, tcf, nil
		}
		tcf()
	}
	if err := iter.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("iterating over pending tasks in %q set: %v", rq.pendingSetKey(), err)
	}
	return nil, nil, nil, nil
}

// Drop takes the ID for a tasks an makes it available
// for pulling from the Queue again. The dropped task
// should be count by implementations as pending
// again, unless it has been previously completed.
// Workers should use this to return to the queue
// tasks they have not completed.
func (rq *redisQ) Drop(ctx context.Context, id string) error {
	tKeyPrefix := rq.taskKeyPrefix(id)
	err := rq.withLockFor(ctx, tKeyPrefix, lockAttempts, func(ctx context.Context) error {
		ok, err := rq.rc.SMove(rq.runningSetKey(), rq.pendingSetKey(), tKeyPrefix).Result()
		if err != nil {
			return fmt.Errorf("moving %q from %q to %q: %v", tKeyPrefix, rq.runningSetKey(), rq.pendingSetKey(), err)
		}
		if !ok {
			return nil
		}
		runningMarkKey := fmt.Sprintf("%s:running", tKeyPrefix)
		_, err = rq.rc.Del(runningMarkKey).Result()
		if err != nil {
			return fmt.Errorf("removing %q: %v", runningMarkKey, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("dropping %s: %v", id, err)
	}
	return nil
}

// Complete takes the ID for a task. Implementations
// should remove the task from the running state.
func (rq *redisQ) Complete(ctx context.Context, id string) error {
	tKeyPrefix := rq.taskKeyPrefix(id)
	err := rq.withLockFor(ctx, tKeyPrefix, lockAttempts, func(ctx context.Context) error {
		count, err := rq.rc.SRem(rq.runningSetKey(), tKeyPrefix).Result()
		if err != nil {
			return fmt.Errorf("removing %q from %q: %v", tKeyPrefix, rq.runningSetKey(), err)
		}
		if count == 0 {
			return nil
		}
		runningMarkKey := fmt.Sprintf("%s:running", tKeyPrefix)
		_, err = rq.rc.Del(runningMarkKey).Result()
		if err != nil {
			return fmt.Errorf("removing %q: %v", runningMarkKey, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("completing %s: %v", id, err)
	}
	return nil
}

// Count returns the number of
// pending and running tasks in the queue
// or an error
func (rq *redisQ) Count(context.Context) (int, int, error) {
	// count pending and running sets at the same time to prevent a task
	// moving between them from triggering a false "work finished" event
	cmd := redis.NewSliceCmd(
		"EVAL",
		`return {redis.call("SCARD", KEYS[1]), redis.call("SCARD", KEYS[2])}`,
		2,
		rq.pendingSetKey(),
		rq.runningSetKey(),
	)
	err := rq.rc.Process(cmd)
	if err != nil {
		return 0, 0, fmt.Errorf("counting tasks: %v", err)
	}
	v, err := cmd.Result()
	if err != nil {
		return 0, 0, fmt.Errorf("counting tasks: %v", err)
	}
	if len(v) != 2 {
		return 0, 0, fmt.Errorf("counting tasks: redis returned %d counts instead of 2", len(v))
	}
	p64, ok := v[0].(int64)
	if !ok {
		return 0, 0, fmt.Errorf("counting tasks: cannot extract integer pending tasks count from %v (%T)", v[0], v[0])
	}
	p := int(p64)
	r64, ok := v[1].(int64)
	if !ok {
		return 0, 0, fmt.Errorf("counting tasks: cannot extract integer running tasks count from %v (%T)", v[1], v[1])
	}
	r := int(r64)
	return p, r, nil
}

// Stops the queue. Implementations should use the
// call to free resources and even cancel pulled
// contexts.
func (rq *redisQ) Stop() error {
	rq.allTaskCF()
	return nil
}

func (rq *redisQ) taskKeyPrefix(taskID string) string {
	return fmt.Sprintf("%s:task:%s", rq.id, taskID)
}

func (rq *redisQ) pendingSetKey() string {
	return fmt.Sprintf("%s:pending", rq.id)
}

func (rq *redisQ) runningSetKey() string {
	return fmt.Sprintf("%s:running", rq.id)
}

func (rq *redisQ) withLockFor(ctx context.Context, taskKeyPrefix string, additionalAttempts int, f func(ctx context.Context) error) error {
	tLockKey := fmt.Sprintf("%s:lock", taskKeyPrefix)
	tLockValue := randString(20)
	lctx, cf := context.WithTimeout(ctx, rq.lockTTL)
	defer cf()
	boolCMD := rq.rc.SetNX(tLockKey, tLockValue, rq.lockTTL)
	ok, err := boolCMD.Result()
	if err != nil {
		return fmt.Errorf("could not acquire lock: %v", err)
	}
	if !ok {
		if additionalAttempts > 0 {
			cf()
			d, _ := rq.rc.TTL(tLockKey).Result()
			time.Sleep(d + time.Duration(rand.Int63n(int64(failToLockSleep)*int64(additionalAttempts))))
			return rq.withLockFor(ctx, taskKeyPrefix, additionalAttempts-1, f)
		}
		return fmt.Errorf("could not acquire lock: already taken")
	}
	defer func() {
		rq.rc.Eval(lockReleaseScript, []string{tLockKey}, tLockValue)
	}()
	return f(lctx)
}

func (rq *redisQ) dropTimedOutTasks() {
	ticker := time.NewTicker(rq.taskMaxRun / 2)
	defer ticker.Stop()
	for {
		iter := rq.rc.SScan(rq.runningSetKey(), 0, "", 0).Iterator()
		for iter.Next() {
			var timedOut bool
			tID := iter.Val()
			rq.withLockFor(rq.allTaskCtx, tID, 0, func(ctx context.Context) error {
				exists, err := rq.rc.Exists(fmt.Sprintf("%s:running", tID)).Result()
				if err != nil {
					return err
				}
				timedOut = !exists
				return nil
			})
			if timedOut {
				tIDTokens := strings.Split(tID, ":")
				tID := tIDTokens[len(tIDTokens)-1]
				rq.Drop(rq.allTaskCtx, tID)
			}
			if rq.allTaskCtx.Err() != nil {
				return
			}
		}
		select {
		case <-rq.allTaskCtx.Done():
			return
		case <-ticker.C:
		}
	}
}
