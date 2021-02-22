package com.vj.first;

import java.util.Comparator;


//In this question I have made two different comparator classes
//One for Double, and other for String
//Here the disadvantage is that we have to make multiple classes whenever we have a new type of object---- but it solvers the problem
//The more simplified version of the code also I have attached with the name QuestioanTwoSimplified--- it is more generic
 

class DoubleCompare implements Comparator<Double> {

	@Override
	public int compare(Double arg0, Double arg1) {
		// TODO Auto-generated method stub
		return arg0.compareTo(arg1);
	}
}

class IntegerCompare implements Comparator<Integer> {

	@Override
	public int compare(Integer arg0, Integer arg1) {
		// TODO Auto-generated method stub
		return arg0.compareTo(arg1);
	}
}

class StringCompare implements Comparator<String> {

	@Override
	public int compare(String arg0, String arg1) {
		// TODO Auto-generated method stub
		return arg0.compareTo(arg1);
	}

}

public class QuestionTwo<T> {

	T[] array;
	Comparator<T> compare;

	public void sort(T[] intArr, Comparator<T> comparator) { // array length must be a power of 2
		this.array = intArr;
		this.compare = comparator;
		sort(0, intArr.length);
	}

	private void sort(int low, int n) {

		if (n > 1) {
			int mid = n >> 1;

			sort(low, mid);
			sort(low + mid, mid);

			combine(low, n, 1);
		}
	}

	private void combine(int low, int n, int st) {

		int m = st << 1;

		if (m < n) {
			combine(low, n, m);
			combine(low + st, n, m);

			for (int i = low + st; i + st < low + n; i += m)
				compareAndSwap(i, i + st);

		} else
			compareAndSwap(low, low + st);
	}

	private void compareAndSwap(int i, int j) {
		if (this.compare.compare(array[i], array[j]) >= 1)
			swap(i, j);
	}

	private void swap(int i, int j) {
		T h = array[i];
		array[i] = array[j];
		array[j] = h;
	}

	public void printArray(Object[] arr,String tag) {
		System.out.println(tag);
		for (Object i : arr) {
			System.out.print(i+" ");
		}
		System.out.println("");
	}

	public static void main(String[] args) {

		System.out.println("Question 2:");

		
		QuestionTwo<Integer> qTI = new QuestionTwo<Integer>();
		Integer[] i_array = { 4, 2, 3, 1,10,11,13,16,14,36,74,18,2345,34,15,98 };
		qTI.printArray(i_array,"Integer Unsorted:");
		IntegerCompare iC = new IntegerCompare();
		qTI.sort(i_array, iC);
		qTI.printArray(qTI.array,"Integer Sorted:");

		QuestionTwo<Double> qTD = new QuestionTwo<Double>();
		Double[] d_array = { 4.0, 2.1, 3.0, 1.7, 10.9, 11.2, 13.8, 16.3, 14.56, 36.7, 74.2, 18.9, 2345.1, 34.4, 15.0,98.1 };
		qTD.printArray(d_array,"Double Unsorted");
		DoubleCompare dC = new DoubleCompare();
		qTD.sort(d_array, dC);
		qTD.printArray(qTD.array,"Double Sorted");
		
		
		QuestionTwo<String> qTS = new QuestionTwo<String>();	
		String[] stringArr = { "A", "C", "Y", "G", "I", "N", "K", "R", "Z", "B", "T", "M", "Q", "F", "D", "L" };
		qTS.printArray(stringArr,"String Unsorted");			
		StringCompare dS = new StringCompare();
		qTS.sort(stringArr, dS);
		qTS.printArray(qTS.array,"String Sorted");
	}

}
