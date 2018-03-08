package com.ybwh.rocketmq.study.commonscli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

/**
 * 
 * commons-cli s是一个小巧的解析命令参数的工具
 * @author fan79
 *
 */
public class CLITest {
	@Test
	public void test() throws ParseException {
		// 模拟命令行参数
		String[] args = { "-dir", ".", "-e", "-t", "3343" };
		testParser(args);
	}

	public void testParser(String[] args) {
		
		try {
			Options options = new Options();
			options.addOption("dir", true, "root folder path.");
			options.addOption("e", false, "file last modify time.");
			options.addOption("t", "time", true, "file last modify time.");

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);
			if (cmd.hasOption("dir")) {
				System.out.println(cmd.getOptionValue("dir"));
			}

			System.out.println(cmd.getOptionValue("t"));
			System.out.println(cmd.getOptionValue("e"));
			
			//自动生成帮助文档
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "mytest", options,true );
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		
	}
}