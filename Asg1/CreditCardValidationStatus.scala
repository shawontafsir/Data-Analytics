/*
 * File:    CreditCardValidationStatus.scala
 *
 * Author:  Khalid Hasan (kh597s@missouristate.edu)
 * Date:    Sep 29, 2023
 * Course:  Data Analytics (CS735)
 *
 * Summary of File:
 *  This file contains a function "isValid" which checks credit card
 *  validation status. The program takes a credit card numbers as string
 *  and then returns the validation status of corresponding card number.
 *
 *  This program introduces a singleton object "CreditValidationStatus"
 *  since we just need utility functions that take the card number as a
 *  parameter to check the status, and we do not need any class instance
 *  to utilize the number later.
 */

object CreditCardValidationStatus {

  /**
   * Calls validation checker method
   *
   * @param number a credit card number
   * @return the validation status
   */
  def apply(number: String): String = {
    isValid(number)
  }

  /**
   * Calculates the sum of digits if there is more than 1 digit
   * For example: 18 -> 9, 7 -> 7
   *
   * @param number an integer value
   * @return the sum of digits
   */
  private def sumOfDigits(number: Int) = {
    var result = 0
    var num = number

    while (num > 0) {
      result += num % 10
      num = num / 10
    }

    result
  }

  /**
   * Simulates the Luhn check or the Mod 10 check
   * Description:
   *  1. Doubles each even placed digit from right to left.
   *     If result > 9, then takes sum of digits to make 1 digit.
   *  2. Adds all digits from step 1.
   *  3. Adds all odd placed digits from right to left.
   *  4. Sums the results from step 2 and 3 to get the total.
   *  5. If total % 10 == 0, returns valid otherwise invalid
   *
   * @param number a credit card number as a string
   * @return the validation status
   */
  private def isValid(number: String): String = {
    /** stores the sum of odd placed digits from right to left */
    var sumOfOddPlacedInReverse = 0

    /** stores the sum of corresponding value from even placed digits from right to left */
    var sumOfEvenPlacedInReverse = 0

    /** runs iteration on card number in reverse order */
    for (i <- number.length - 1 to 0 by -1) {
      val digit = number(i).asDigit

      /** checks if the place of current digit is odd or even */
      if ((number.length - i) % 2 == 1) {
        /** takes the odd placed digit without any change */
        sumOfOddPlacedInReverse += digit
      }
      else {
        /** doubles the even placed digit and takes the sum of resultant digits */
        sumOfEvenPlacedInReverse += sumOfDigits(digit * 2)
      }
    }

    val totalSum = sumOfOddPlacedInReverse + sumOfEvenPlacedInReverse

    if (totalSum % 10 == 0) "valid" else "invalid"
  }
}