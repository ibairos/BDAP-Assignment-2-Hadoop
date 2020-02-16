#! /bin/sh

mvn clean
mvn install


scp /mnt/Datos/UNI/KUL/Semester\ 1/Big\ Data\ Analytics\ Programming/Assignments/A2/BDAP-Assignment-2-Hadoop/target/BDAP-Assignment-2-1.0.jar bierbeek.cs.kotnet.kuleuven.be:BDAP-Assignment-2/Hadoop/
