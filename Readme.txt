1) Introduction
----------------
First of all, thanks team Casetext for giving me an opportunity to work on the coding challenge. The goal of this project is to solve Casetext's code challenge problem in which we have to extract citations from 133,455 text files that represent every US supreme court case till present. The output of the solution is a csv file containing (filename,citation,count) tuples. 

2) Approach, Challenges & Thoughts
-----------------------------------
The approach taken to solve this problem is to leverage the power of regular expressions to identify and count different forms of citations used in legal documents. The problem statement mentions 3 major forms of citations used in legal documents (main citation, main citation with a pincite and short citation). Regular expressions are used to identify all 3 types of citations and further processing is done to correctly map pincites and short citations to the correct main citation (See code for details). 

The main challenge that I felt after seeing the problem statement was the somewhat large size of the data. There are 133,455 files in total which occupy nearly 1 GB space on the hard disk. To process all these files (and perhaps more files in future) efficiently, I decided to use Apache Spark framework to process these files. So rather than putting effort in researching more sophisticated NLP techniques than regular expressions to process text, in this first iteration, I decided to put effort on making use of Spark framework to process the data. Spark presented some of its own challenges while trying to leverage this framework, but in the end all the effort paid off and the code ran successfully. I think now that we have Spark fraemwork up and running, in the next iteration, we can research more sophisticated techniques to improve accuracy of our results. 

3) Code Description
--------------------
Scala language is used to solve this challenge. This fits well with Spark framework as Spark is written in Scala. The project and dependencies are managed using Maven. I used Spark 1.5.0 with Scala 2.10 for this project. The code is present at location: https://github.com/judahk/casetext-hw.

The two main classes are TestCountCitations.scala (this is an object actually and contains main function) and CountCitations.scala which is the class that extracts and counts citations in the data. See code for details. 

The code was developed on Windows 7. To run the code:
1. Pull code repository from Github (url: https://github.com/judahk/casetext-hw)
2. Build code with Maven 
 2a) You need Maven installed.
 2b) Run command: mvn clean package -DskipTests
3. Command is 2b creates a jar file in the target folder inside project folder
4. Download Spark as instructued on the Spark website (url: http://spark.apache.org/downloads.html). I downloaded Spark prebuilt for Hadoop 2.6. 
5. cd to the 'bin' folder inside the Spark folder. Run code with command (on windows):
   spark-submit.cmd --driver-memory 7g --class com.casetext.citations.TestCountCitations --master local[*] C:\Users\judahk\workspace\casetext_hw\casetext-app\target\casetext-app-1.0-SNAPSHOT.jar C:\Users\judahk\workspace\casetext_hw\casetext-app\data\446c8aa0-6eba-11e5-bc7f-4851b79b387c C:\Users\judahk\workspace\casetext_hw\data\reporters.txt C:\Users\judahk\workspace\casetext_hw\casetext-app\output
   
   where paths above are paths to project jar, data folder, reporters file and output dir resp.
6. I hope above process works, but if not please ask me doubts. 

4) Output
----------
The result is in the file result.csv (in the output folder inside the repo)
