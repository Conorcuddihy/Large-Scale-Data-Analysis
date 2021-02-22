package com.vj.first;

public class QuestionOne {

	int[] array;

	public void sort(int[] array) { // array length must be a power of 2
		this.array = array;
		sort(0, array.length);
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
		if (array[i] > array[j])
			swap(i, j);
	}

	private void swap(int i, int j) {
		int h = array[i];
		array[i] = array[j];
		array[j] = h;
	}

	public static void main(String[] args) {
		System.out.println("Question 1:");
		int[] arr = { 4, 2, 3, 1,10,11,13,16,14,36,74,18,2345,34,15,98 };
		System.out.println("Input:");
		for (int i : arr) {
			System.out.print(i);
			System.out.print(' ');
		}
		QuestionOne d = new QuestionOne();
		d.sort(arr);
		System.out.println("\nOutput:");
		for (int i : d.array) {
			System.out.print(i);
			System.out.print(' ');
		}
	}

}
