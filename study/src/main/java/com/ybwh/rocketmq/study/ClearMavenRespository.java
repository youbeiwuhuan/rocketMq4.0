package com.ybwh.rocketmq.study;

import java.io.File;

public class ClearMavenRespository {
	
	public static void main(String[] args) {
		String mavenRespositoryPath = "C:\\Users\\fan79\\.m2\\repository";
		deleteLastupdateFile(new File(mavenRespositoryPath));
		
		System.out.println("clear compelete!!!");
	}

	private static void deleteLastupdateFile(File mavenRespositoryDir) {
		
		for (File f : mavenRespositoryDir.listFiles()) {
			if (f.isDirectory()) {
				deleteLastupdateFile(f);
			}else if (f.getName().endsWith(".lastUpdated")) {
				System.out.println("delete file :"+f.getName());
				f.delete();
			}
		}
	}

}
