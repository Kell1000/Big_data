import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MoyenneHauteurArbresDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// vérifier les paramètres fournis au lancement du mapper
        if (args.length != 2) {
            System.err.println("Usage: ModeleMapReduceDriver <input path> <outputpath>");
            System.exit(-1);
        }

        // instant de démarrage
        Long initTime = System.currentTimeMillis();

        // créer le job map-reduce à l'aide de YarnJob qui vérifie sa structure
        Configuration conf = this.getConf();
        YarnJob job = new YarnJob(conf, "Moyenne Hauteur Arbres Job");
        job.setJarByClass(MoyenneHauteurArbresDriver.class);
        
     // définir les classes Mapper, Reducer et Combiner si nécessaire
        job.setMapperClass(MoyenneHauteurArbresMapper.class);
        job.setReducerClass(MoyenneHauteurArbresReducer.class);
        //job.setCombinerClass(MoyenneHauteurArbresCombiner.class);
        
     // définir les données d'entrée
        job.setInputFormatClass(TextInputFormat.class);
        job.addInputPath(new Path(args[0]));   
        job.setInputDirRecursive(false);       // mettre true si les fichiers sont dans des sous-dossiers

        // définir les données de sortie
        job.job.setOutputKeyClass(Text.class);
        job.job.setOutputValueClass(FloatWritable.class);
        job.setOutputPath(new Path(args[1])); 
     // lancer le job et attendre sa fin en comptant les secondes
        Long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        Long endTime = System.currentTimeMillis();
        System.out.println("Job Duration seconds: " + ((endTime-startTime)/1000L));
        System.out.println("Total Duration seconds: " + ((endTime-initTime)/1000L));
        return success ? 0 : 1;
	}

    public static void main(String[] args) throws Exception
    {
        // préparer et lancer un job en lui passant les paramètres
    	MoyenneHauteurArbresDriver driver = new MoyenneHauteurArbresDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }

}
