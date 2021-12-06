# NYT API Data Loader

## Running Program
```bash
python3 delivery_challenge.py 
```
The program will continue running until it batches all results from the given query.
It will pause for 24 hours once the daily limit is reached. 

## Description

```python
requestArticles(self, pageNum)
```
Given a page number which initializes at 0 upon creating an instance of the NYTimesSource class,
the method will make an API call only if the daily limit of calls has not been reached.
If the daily limit has been reached the method sleeps for 24 hours until it can reset the call count.

- API responses['docs'] are stored in an instance variable Queue that is accessible by getDataBatch()

```python
flattenData(self, dat_dict, sep=".")
```
Given a single Dictionary object, it uses an inner recursive function to flatten a dictionary
by checking the instance of the inner element, recursion stops when the isinstance is not a dict or list

```python
getDataBatch(self, batch_size)
```
Given a batch size, this method validates if the Queue of records contains enough records to generate a batch
if it does not it calls requestArticles() to populate the queue with more records

It flattens enough records to meet the batch_size and then yields the batch. process continues until there are
no more records from the initial keyword query. 

```python
getSchema(self)
```
If the record queue is empty it calls requestArticles and then flattenData in order to return all keys
as a schema list.

```python
waitForTomorrow(self)
```
This method sleeps for 24 hours and is activated by the requestArticles() accordingly