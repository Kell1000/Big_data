import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class MoyenneHauteurArbresMapper extends 
	Mapper<LongWritable, Text, Text, MoyenneWritable> {
	
	// allocation mémoire de la clé et valeur de sortie
    private Text cleI = new Text();
    private MoyenneWritable valeurI = new MoyenneWritable();
	
	@Override
	public void map(LongWritable cleE, Text valeurE, Context context) throws IOException, InterruptedException {
		Arbre.fromLine(valeurE.toString());
		cleI.set( Arbre.getGenre());
		valeurI.set(Arbre.getHauteur());
		context.write(cleI, valeurI);
	}
}