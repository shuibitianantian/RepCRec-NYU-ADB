## RepCRec-NYU-ADB

###### A simple Replicated Concurrency Control and Recovery algorithm (RepCRec)

##### TODO: 
* More testing and save tests result in specific folder

## How to run?
1. Make sure you have installed Python3.x  
2. Run `pip install -r requirements.txt` to install dependencies
3. Run `python main.py [f|d|i] -input {path/to/input} -output {path/to/output}`
> `f: The input source is a file contains single test case or multiple test cases which is separated by <END>, -input is the file path, -output is the path to save result`
>
> `d: The input source is a directory contains some test files, -input is the directory, -output is the directory to save the result`
>
> `i: Interactive mode, user can enter operation line by line`

## Test file
* Run `python main.py f -input {path/to/input_file} -output {path/to/result_file}`

## Test directory
* Run `python main.py d -input {path/to/input_directory} -output {path/to/result_directory}`

## Interactive Mode
* Run `python main.py i` in the folder of `./RepCRec-NYU-ADB`
* Interactive mode will initialize sites by default.
>`refresh` command will reinitialize site status 
>
>`<END>` command indicates the end of a test case.
>
>`quit` will exit program 

## Documentation


