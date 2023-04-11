import java.io.IOException;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Driver {

	public static void main(String[] args) throws Exception {
	    Scanner scanner = new Scanner(System.in);
	    System.out.println("Enter 1)Popular Movies or 2)Genre Classification");
	    int choice = scanner.nextInt();

	    switch (choice) {
	      case 1:
	        PopularMovies.run();
	        break;
	      case 2:
	        GenreClassification.run();
	        break;
	      default:
	        System.out.println("Invalid choice");
	    }
	  }
}
