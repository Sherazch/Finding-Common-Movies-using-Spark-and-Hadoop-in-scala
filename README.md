# Finding-Common-Movies-using-Spark-and-Hadoop-in-scala
spark code that finds all pairs of users who watched more than 50 common movies and sort them by the number of common movies they watched


 We are given two input files:  
 
1.ratings dataset containing the following information. Each row in this dataset is in the following format: 
 
User_id movie_id1#movie_id2#movie_id3,….  
 
The first column is the id of a user, and the second column is the ids of all the movies that this user watched. The movie ids are separated by ‘#’.  
 
2.Movies dataset. This file contains movies information and has the following format (genres are delimited by “|”) : 
 
Movie_id#movie_title#genre1|genre2|genre3|… 
 
Write a spark code that finds all pairs of users who watched more than 50 common movies and sort them by the number of common movies they watched. Your output should be in the following format:  
 
User_id1, user_id2  count_of_common_movies a comma-separated list of the titles of the common movies watched by user_id1 and user_id2  
 
