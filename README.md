# Hive UDF

# DESCRIPTION

The function checks for a given string in an a array of strings. It takes two arguments as input - an array and a string, returns true if the given string exists and returns false otherwise.


# COMPILE

compile the project using 

==> mvn compile 

# BUILD

build the project using 

==> mvn package


# HIVE Commandline

enter the hive shell

==> hive 

Add the JAR file

hive> ADD JAR /path/to/ContainsString.jar;

Create a temporary function

hive> create temporary function hasString as 'com.sadhana.hiveudf';

Use the function 

SELECT ContainsString(col_list[], Stringtobefound) FROM <table_name>;




