# vermeerDB
A new database built upon MIT 6.830's simpleDB

# Design and API changes:

For testing the `SELECT * FROM some_data_file` query equivalence in the database, we did not create a test.java in the main folder. We decided to implement a seperate `junit` `test` under the package `customTest`. We called it `SelectTest.java`. We also stored the data file of this table under the package `testData`. The reason is to allow us to create new `customTests` that we might write, and to keep new data files all in a single consistent place. 