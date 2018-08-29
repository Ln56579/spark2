package com.huoshan.java.Thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 * @description 线程池     提交的10次任务    只有一个线程
 * @author ln56
 *
 */
public class BufferThreadPoolDemo {
	public static void main(String[] args) {
		//可以有多个线程(可缓存的线程池)  --- >  切换的
		ExecutorService pool = Executors.newCachedThreadPool();

		for (int i = 1; i <= 20 ; i++) { //提交了10,一次一次执行

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

			//pool.submit(task)       有无返回
		}

		System.out.println(Thread.currentThread().getName()+"all Task is submited");
		//pool.shutdownNow();
	}
}
