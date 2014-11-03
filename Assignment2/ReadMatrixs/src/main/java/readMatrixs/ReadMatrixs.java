package readMatrixs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ReadMatrixs {
	public final static String MATRIX_FILE = ".\\output\\matrixFile.txt";
	public final static String TOKEN = ",";
	
	public static void ReadMatrix(final String originalMatrixFile, String matrixName) throws IOException{				
		BufferedWriter out;
		File file = new File(MATRIX_FILE);
		if(file.exists()){
			out = new BufferedWriter(new FileWriter(MATRIX_FILE, true));	
		}else{
			out = new BufferedWriter(new FileWriter(MATRIX_FILE, false));	
		}
		
		RandomAccessFile in = new RandomAccessFile(originalMatrixFile, "r");
		String firstLine = in.readLine();
		String elements[] = firstLine.split("\t");
		int colNum = Integer.valueOf(elements[1]);
		int count = 0;
		
		try{				
			while(true){
				double value = in.readDouble();
				
				int i = count/colNum;
				int j = count%colNum;
				
				String line = matrixName + TOKEN + String.valueOf(i) + TOKEN + String.valueOf(j) + TOKEN + String.valueOf(value) + "\n";				
				out.write(line);
				
				count++;
			}
			
		}catch(IOException e1){
			//do nothing
		}finally{
			System.out.println(count);
			in.close();
			out.close();
		}
	}
	
	public static void main(String[] args) throws Exception{
		ReadMatrix(".\\input\\matrix_1000_100.dat", "M");
		ReadMatrix(".\\input\\matrix_100_1000.dat", "N");		
	}
}
