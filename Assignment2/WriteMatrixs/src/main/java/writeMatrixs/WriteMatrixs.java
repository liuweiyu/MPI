package writeMatrixs;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

public class WriteMatrixs {
	public final static String TMP_OUTPUT_FILE = ".\\input\\tmpOutputFile.txt";
	public final static String RESULT_FILE = ".\\output\\result.dat";
	public final static String TOKEN = ",";
	
	public final static int COL_NUM = 1000;
	public final static int ROW_NUM = 1000;

	public static void WriteMatrixIntoBinaryForm(final String rltFN, final int rowNum, final int colNum) throws IOException{
		RandomAccessFile in = new RandomAccessFile(TMP_OUTPUT_FILE, "r");
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(rltFN));
		writer.write(String.valueOf(rowNum));
		writer.write(String.valueOf('\t'));
		writer.write(String.valueOf(colNum));
		writer.write(String.valueOf('\n'));
		writer.close();		
		
		FileOutputStream fos = new FileOutputStream(rltFN, true);
		DataOutputStream dos = new DataOutputStream(fos);
		
		try{
			while(true){
				String line = in.readLine();
				String strValue = (line.split(TOKEN))[2];
				double dblValue = Double.valueOf(strValue);
				dos.writeDouble(dblValue);
			}
		}catch(IOException e1){
			//do nothing
		}finally{
			in.close();
			dos.close();
		}		
	}
	
	public static void main(String[] args) throws Exception{		
		WriteMatrixIntoBinaryForm(RESULT_FILE, ROW_NUM, COL_NUM);
	}
}
