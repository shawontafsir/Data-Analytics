The Readme.txt file contains a brief summary of each included file and how to run the program.

Main.scala
This file contains main function of the application. The program reads credit card numbers from a file source as strings
and then call CreditCardValidationStatus to display the validation status of corresponding card number to standard
output (i.e. computer console).


CreditCardValidationStatus.scala
This file contains a function "isValid" which checks credit card validation status. The program takes a credit card
numbers as string and then returns the validation status of corresponding card number. This program introduces a
singleton object "CreditValidationStatus" since we just need utility functions that take the card number as a parameter
to check the status, and we do not need any class instance to utilize the number later.


numbers.txt
This file consists of a set of card numbers to use as an input of the program.


output.png
This is a screenshot of outputs generated for the input file "numbers.txt" by the program.


How to run the program:
1. Compile the two scala files by running the following "scalac" command:
    scalac Main.scala CreditCardValidationStatus.scala
2. Run the following "scala" command:
    scala Main
