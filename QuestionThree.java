package com.vj.first;

import java.util.Comparator;

public class QuestionThree<T> {

	T[] array;
	Comparator<T> compare;
	public void sort(T[] intArr, Comparator<T> comparator) { // array length must be a power of 2
		this.array = intArr;
		this.compare=comparator;
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
		if (this.compare.compare(array[i], array[j])>=1)
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

		System.out.println("Question 3:");


		QuestionThree<Double> qTD = new QuestionThree<Double>();
		Double[] d_array = { 4.0, 2.1, 3.0, 1.7, 10.9, 11.2, 13.8, 16.3, 14.56, 36.7, 74.2, 18.9, 2345.1, 34.4, 15.0,98.1 };
		qTD.printArray(d_array,"Double Unsorted:");
		qTD.sort(d_array, (arg0,arg1) -> arg0.compareTo(arg1));
		qTD.printArray(qTD.array,"Double Sorted:");
		
		
		QuestionThree<String> qTS = new QuestionThree<String>();
		String[] stringArr = { "A", "C", "Y", "G", "I", "N", "K", "R", "Z", "B", "T", "M", "Q", "F", "D", "L" };
		qTS.printArray(stringArr,"String Unsorted:");
		qTS.sort(stringArr, (arg0,arg1) -> arg0.compareTo(arg1));
		qTS.printArray(qTS.array,"String Sorted:");
	}

}
