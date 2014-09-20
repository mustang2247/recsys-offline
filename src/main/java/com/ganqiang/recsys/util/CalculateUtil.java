package com.ganqiang.recsys.util;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CalculateUtil
{
  public static void main(String[] args)
  {
    String str = "0.5555555555557";
    System.out.println(parseExp("0"));
  }
  

  public static boolean isNumeric(String str){ 
    Pattern p = Pattern.compile("^(-?\\d+)(\\.\\d+)?$");  
    Matcher m = p.matcher(str);  
    boolean isNumber = m.matches();  
    return isNumber;    
  }

  public static String parseExp(String expression)
  {
	  if (expression.equals("0")){
		  return "0";
	  }
    // String numberReg="^((?!0)\\d+(\\.\\d+(?<!0))?)|(0\\.\\d+(?<!0))$";
    expression = expression.replaceAll("\\s+", "").replaceAll("^\\((.+)\\)$",
        "$1");
//    String checkExp = "\\d";
    String minExp = "^((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\]))[\\+\\-\\*\\/]((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\]))$";
    // 最小表达式计算
    if (expression.matches(minExp)) {
      String result = calculate(expression);

      return Double.parseDouble(result) >= 0 ? result : "[" + result + "]";
    }
    // 计算不带括号的四则运算
    String noParentheses = "^[^\\(\\)]+$";
    String priorOperatorExp = "(((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\]))[\\*\\/]((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\])))";
    String operatorExp = "(((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\]))[\\+\\-]((\\d+(\\.\\d+)?)|(\\[\\-\\d+(\\.\\d+)?\\])))";
    if (expression.matches(noParentheses)) {
      Pattern patt = Pattern.compile(priorOperatorExp);
      Matcher mat = patt.matcher(expression);
      if (mat.find()) {
        String tempMinExp = mat.group();
        expression = expression.replaceFirst(priorOperatorExp,
            parseExp(tempMinExp));
      } else {
        patt = Pattern.compile(operatorExp);
        mat = patt.matcher(expression);

        if (mat.find()) {
          String tempMinExp = mat.group();
          expression = expression.replaceFirst(operatorExp,
              parseExp(tempMinExp));
        }
      }
      return parseExp(expression);
    }
    // 计算带括号的四则运算
    String minParentheses = "\\([^\\(\\)]+\\)";
    Pattern patt = Pattern.compile(minParentheses);
    Matcher mat = patt.matcher(expression);
    if (mat.find()) {
      String tempMinExp = mat.group();
      expression = expression
          .replaceFirst(minParentheses, parseExp(tempMinExp));
    }
    return parseExp(expression);
  }

  public static String calculate(String exp)
  {
    exp = exp.replaceAll("[\\[\\]]", "");
    String number[] = exp.replaceFirst("(\\d)[\\+\\-\\*\\/]", "$1,").split(",");
    BigDecimal number1 = new BigDecimal(number[0]);
    BigDecimal number2 = new BigDecimal(number[1]);
    BigDecimal result = null;

    String operator = exp.replaceFirst("^.*\\d([\\+\\-\\*\\/]).+$", "$1");
    if ("+".equals(operator)) {
      result = number1.add(number2);
    } else if ("-".equals(operator)) {
      result = number1.subtract(number2);
    } else if ("*".equals(operator)) {
      result = number1.multiply(number2);
    } else if ("/".equals(operator)) {
      result = number1.divide(number2);
    }

    return result != null ? result.toString() : null;
  }

  public static double add(double v1, double v2)
  {
    BigDecimal b1 = new BigDecimal(Double.toString(v1));
    BigDecimal b2 = new BigDecimal(Double.toString(v2));
    return b1.add(b2).doubleValue();
  }

  public static double sub(double v1, double v2)
  {
    BigDecimal b1 = new BigDecimal(Double.toString(v1));
    BigDecimal b2 = new BigDecimal(Double.toString(v2));
    return b1.subtract(b2).doubleValue();
  }

  public static double mul(double v1, double v2)
  {
    BigDecimal b1 = new BigDecimal(Double.toString(v1));
    BigDecimal b2 = new BigDecimal(Double.toString(v2));
    return b1.multiply(b2).doubleValue();
  }

  public static double div(double v1, double v2, int scale)
  {
    if (scale < 0) {
      throw new IllegalArgumentException(
          "The scale must be a positive integer or zero");
    }
    BigDecimal b1 = new BigDecimal(Double.toString(v1));
    BigDecimal b2 = new BigDecimal(Double.toString(v2));
    return b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
  }
  
  public static double div(double v1, double v2)
  {
    BigDecimal b1 = new BigDecimal(Double.toString(v1));
    BigDecimal b2 = new BigDecimal(Double.toString(v2));
    return b1.divide(b2, 0, BigDecimal.ROUND_DOWN).doubleValue();
  }

  public static int getIntHalfValue(double number)
  {
    BigDecimal b = new BigDecimal(number);
    return b.setScale(0, BigDecimal.ROUND_HALF_UP).intValue();
  }

  public static double getDoubleHalfValue(double number)
  {
    BigDecimal b = new BigDecimal(number);
    return b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
  }
  
  public static double getDoubleHalfValue(double number, int scale)
  {
    BigDecimal b = new BigDecimal(number);
    return b.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
  }

  /**
   * 计算等额本息还款
   * 
   * @param money
   *          总贷款数额
   * @param rate
   *          年利率
   * @param month
   *          分期月份数量
   * @return 每月还款金额
   */
  public static double getPayPerMonth(double money, double rate, int month)
  {
    double pay;
    double date = rate / 12;// 月利率=年利率/12
    pay = (money * date * Math.pow(1 + date, month))
        / (Math.pow(1 + date, month) - 1);
    return getDoubleHalfValue(pay);
  }

  // public static double getRemainMoney(double money, double rate, int month){
  //
  // }

}
