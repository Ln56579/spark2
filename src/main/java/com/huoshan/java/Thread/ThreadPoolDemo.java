package com.huoshan.java.Thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * @description 线程池     提交的10次任务    只有一个线程
 * @author ln56
 *
 */
public class ThreadPoolDemo {
	public static void main(String[] args) {
		//一个线程
		//ExecutorService pool = Executors.newSingleThreadExecutor();

		//固定大小的线程池
		ExecutorService pool = Executors.newFixedThreadPool(5);

		for (int i = 1; i <= 10 ; i++) { //提交了10,一次一次执行

			pool.execute(new Runnable() {

				@Override
				public void run() {
					System.out.println(Thread.currentThread().getName());
					try {
						Thread.sleep(1000);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}

		System.out.println(Thread.currentThread().getName()+"all Task is submited");
		//pool.shutdownNow();
	}
}
