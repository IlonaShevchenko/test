# BigData Diploma Project Task
## Use PySpark!

### Aim
Hi there! As a result of building current project you will be able to:
- Work with one of the most popular and trending frameworks Apache Spark. 
- Compose different queries to distributed data using SQL. 
- Get practical knowledge of the world-top VCS (Version Control System) Git and Repository Manager GitLab. - *Understand how to write unit tests for your code. 
- Look at the open source data 
- Accomplish the documentation of your code.

### Tasks
#### Preparation stage
1. Choose an appropriate programming language for Spark. Read the docs
and useful resources. Pay careful attention to SQL modules.
2. Install Spark.
3. Create a Git repository on GitLab or Github and make sure it’s public or
accessible by mentor’s email. Please, use meaningful names for your
repository (e.g. imdb-spark-project).
4. Create 2 branches: main (old master) and develop (from main). It is made
to enable teamwork and double-checking. In the future you will create
your branches for each small piece of functionality from develop and
create merge requests to develop (review process with your teammate).
5. Create a project locally. Please, use a meaningful name as below.
6. Add file .gitignore to your project. [23]
7. Download IMDB datasets to your PC. Do not put them into the project
repository.
8. Look at the datasets. Explain, what data of what types do we have? It will
be useful for further development.

#### Setup Stage
1. Depending on the language you chose, follow appropriate instructions to
set up Spark. [24]
2. Try to create a test DataFrame and apply .show() method on it. If your
data is shown as expected, congrats! We can move forward! If not, just
manage the problem. Try to resolve the issue by yourself, but don’t
spend more than a couple of hours on it. In any case, if you and your
teammate are stuck, text your tutor and ask for help.

#### Extraction Stage
Here is the place where all the magic starts.
1. Create appropriate schemas for all 7 datasets.
2. Depending on the schemas above create corresponding DataFrames by
reading data from the PC.
3. Check if everything went right with any method on DataFrames.
4. *Wrap it up in a function and write unit tests for this function.

#### Transformation Stage
Hint: write your transformations as functions. It will be much
easier to test particular functionality.
1. Get all titles of series/movies etc. that are available in Ukrainian.
2. Get the list of people’s names, who were born in the 19th century.
3. Get titles of all movies that last more than 2 hours.
4. Get names of people, corresponding movies/series and characters they
played in those films.
5. Get information about how many adult movies/series etc. there are per
region. Get the top 100 of them from the region with the biggest count to
the region with the smallest one.
6. Get information about how many episodes in each TV Series. Get the top
50 of them starting from the TV Series with the biggest quantity of
episodes.
7. Get 10 titles of the most popular movies/series etc. by each decade.
8. Get 10 titles of the most popular movies/series etc. by each genre.

#### Loading Stage
1. Load all results of transformations to .csv files. Pay attention: you have to
get ONE .csv file per ONE transformation (8 files overall). Do not push
them into the repository (add your data folder to .gitignore).
 
### *Extra task. Data Modelling.
Use pyspark mllib to build a linear regression model to predict the ratings of
the movies.
