# Parallelize an operation on columns of a dataset on multiple cores using multiprocessing

***Divides a csv file into batches and passes through function of choice to get the desired column output***

For eg:
If we have a very big dataset with 3 columns and we want a fourth column which uses the first 3 columns, performs some operations defined in a target function and gives the fourth output column, we can parallelize this operation using code provided in the parallelize.py file.

Here a target function which multiplies the first 3 columns for demonstration purpose is shown.
