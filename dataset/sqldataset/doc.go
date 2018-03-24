/*
Package sqldataset provides implementations of dataset.Dataset
that use SQL database as backends.

The dataset uses 2 database tables:
  * One for storing discrete values
  * One for the samples

Samples are stored on the samples database, with
their discrete values as references to values in the
discrete value table.
*/
package sqldataset
