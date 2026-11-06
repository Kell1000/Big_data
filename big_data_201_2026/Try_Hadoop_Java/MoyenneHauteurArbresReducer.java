
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MoyenneHauteurArbresReducer extends 
Reducer<Text, MoyenneWritable, Text, DoubleWritable> {
	
	// allocation mémoire de la clé et valeur de sortie
    private Text cleS;
    DoubleWritable valeurS = new DoubleWritable();
	@Override
	public void reduce(Text cleI, Iterable<MoyenneWritable> valeursI, Context context) throws IOException, InterruptedException {
		
		// définir la clé de sortie
        cleS = cleI;
    	MoyenneWritable moyenne = new MoyenneWritable();
		for (MoyenneWritable moy : valeursI) {
			moyenne.add(moy);
		}
		valeurS = new DoubleWritable(moyenne.getMoyenne());
		context.write(cleI, valeurS);
	}
}