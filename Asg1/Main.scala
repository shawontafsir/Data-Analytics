/*
 * File:    Main.scala
 *
 * Author:  Khalid Hasan (kh597s@missouristate.edu)
 * Date:    Sep 29, 2023
 * Course:  Data Analytics (CS735)
 *
 * Summary of File:
 *  This file contains main function of the application.
 *
 *  The program reads credit card numbers from a file source as strings
 *  and then call CreditCardValidationStatus to display the validation
 *  status of corresponding card number to standard output (i.e. computer console).
 */

import scala.io.Source

object Main {
  private val filePath = "./numbers.txt"

  def main(args: Array[String]): Unit = {
    /** reads from file source */
    val fSource = Source.fromFile(filePath);

    for (number <- fSource.getLines()) {
      println(number, CreditCardValidationStatus(number))
    }

    fSource.close()
  }
}