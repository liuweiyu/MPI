package generateRandomMatrix;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.Random;
import java.io.IOException;

public class GenerateRandomMatrix {
	final static int MAX_ENTRY_VALUE = 20;
	
	public static void main(String[] args) {
		int rowNum = Integer.valueOf(args[0]);
		int colNum = Integer.valueOf(args[1]);
		
//		int rowNum = 1000;
//		int colNum = 100;
		
		String fileName = "matrix_" + rowNum + "_" + colNum + ".dat";
		
		try{
		BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
		writer.write(String.valueOf(rowNum));
		writer.write(String.valueOf('\t'));
		writer.write(String.valueOf(colNum));
		writer.write(String.valueOf('\n'));
		writer.close();
		
		Random rand = new Random();
		double dblArray[] = new double[rowNum * colNum];
		FileOutputStream fos = new FileOutputStream(fileName, true);
		DataOutputStream dos = new DataOutputStream(fos);
		
		for(int i = 0; i<rowNum * colNum; i++){
			dblArray[i] = rand.nextDouble() * (MAX_ENTRY_VALUE);
			dos.writeDouble(dblArray[i]);
		}
		
		dos.close();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}		
	}
}
