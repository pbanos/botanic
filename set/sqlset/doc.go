/*
Package sqlset provides implementations of set.Set
that use SQL database as backends.

The set uses 2 database tables:
  * One for storing discrete values
  * One for the samples

Samples are stored on the samples database, with
their discrete values as references to values in the
discrete value table.
*/
package sqlset
