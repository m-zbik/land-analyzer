# land-analyzer
This is a draft of the quick solution for the interview

## How to start the program:

1. clone the repo https://github.com/m-zbik/land-analyzer.git
2. cd ./land_analyzer
3. run docker (tested on version 3.2.1)
./bin/init-docker.sh
4. create a new Bash session inside the container:
docker container exec -it land-analyzer /bin/bash
5. cd src
6. run main.py <comapny_id>
example:
python main.py CR995643170992

you should see the result in a form of printed tree

Please notice that if there were no land count for specific company id following description was provided:
"No information about land parcels ownership number"

#### Additional information:
1. final table has been prepared and stored. This table contains hierarchical dictionary for each company id
2. if you would like to rebuild the table than run main passing one more argument -rebuild (it will take some time)
example:
python main.py R1234987 rebuild


## General idea
- Read data and prepare enriched tables for serving console interface.
- Data preparation was focused on getting main information for reporting:
-- count of lands
-- hierarchical structure 



## My approach:
Based on the task I prepared very high level approach.
I was exploring the data to understand its structure (I start with with exploring not with unit tests as I was not familiar with the data itself)
During the exploration part I have prepared a set of simple functions.
I used a set of simple functions to fit into high level design.
At this point I would start with the unit tests. (On the day to day basis I imagine I would have more understand of the data and what I need to develop so the unit test process will start earlier than this stage).
Code cleaning


## Things to improve:

- Solution is very much customised - it should be more generic and have more object oriented approach. Causes of time I leave it in this shape just to have an understanding of the approach

- Apache Spark has been used here to show the potential solution. It is not optimised and for this specific data set it is less performant then any other approach. Especially concerning is reqursive queering of the data frame with collect action in it. As mentioned is not for performance but general concept.

- Console interaction was made in a very simple way

- Not all the code is commented. Only parts to show the general approach.

- Not all the unit tests are done. It's just to show the approach. 
