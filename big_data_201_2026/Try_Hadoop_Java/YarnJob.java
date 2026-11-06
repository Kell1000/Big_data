import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Cette classe permet de vérifier un job MapReduce : types des paramètres
 * génériques des classes Mapper, Reducer et Combiner
 *
 * @author Pierre Nerzic - pierre.nerzic@univ-rennes1.fr
 *
 * usage:
 * YarnJob job = new YarnJob(conf, "nom");
 * job.setJarByClass(MyDriver.class);
 * job.setMapperClass(MyMapper.class);
 * job.setCombinerClass(MyCombiner.class);
 * job.setReducerClass(MyReducer.class);
 * job.setInputDirRecursive(false);
 * job.addInputPath(new Path(...));
 * job.setInputFormatClass(TextInputFormat.class);
 * job.setOutputPath(new Path(...));
 * job.setOutputFormatClass(TextOutputFormat.class);
 * boolean success = job.waitForCompletion(true);
 */
public class YarnJob
{
    // pour écrire les messages d'erreur en couleur
    public static final String ANSI_RESET    = "\u001B[0m";
    public static final String ANSI_RED      = "\u001B[31m";
    public static final String ANSI_RED_BOLD = "\u001B[31;1m";

    // job Yarn contrôlé par cette classe
    public Job job;

    // tableaux des types qui paramètrent le mapper, le reducer, etc.
    private Type[] inputGenericTypes;
    private Type[] mapperGenericTypes;
    private Type[] combinerGenericTypes;
    private Type[] reducerGenericTypes;


    /**
     * constructeur
     * @param conf configuration Yarn, utiliser this.getConf() dans une instance de Configured
     * @para jobName nom du job, visible dans les logs
     */
    public YarnJob(Configuration conf, String jobName) throws IOException
    {
        job = Job.getInstance(conf, jobName);
    }


    /**
     * Définit le jar du job, ce jar contient le mapper, le reducer, le combiner et le job lui-même
     *
     * voir {@link org.apache.hadoop.mapreduce.Job#setJarByClass(Class<?> cls) setJarByClass}
     */
    public void setJarByClass(Class<? extends Configured> classe)
    {
        job.setJarByClass(classe);
    }


    /**
     * Définit la classe du mapper
     *
     * voir {@link org.apache.hadoop.mapreduce.Job#setMapperClass(Class<? extends Mapper> cls) setMapperClass}
     */
    public void setMapperClass(Class<? extends Mapper<? extends Writable,? extends Writable,? extends Writable,? extends Writable>> classe)
    {
        // définir la classe du mapper
        job.setMapperClass(classe);

        // obtenir les types génériques du mapper
        ParameterizedType superclasse = (ParameterizedType) classe.getGenericSuperclass();
        mapperGenericTypes = superclasse.getActualTypeArguments();

    }


    /**
     * Définit la classe du combiner
     *
     * voir {@link org.apache.hadoop.mapreduce.Job#setCombinerClass(Class<? extends Reducer> cls) setCombinerClass}
     */
    public void setCombinerClass(Class<? extends Reducer<? extends Writable,? extends Writable,? extends Writable,? extends Writable>> classe) throws Exception
    {
        job.setCombinerClass(classe);

        // obtenir les types génériques du combiner
        ParameterizedType superclasse = (ParameterizedType) classe.getGenericSuperclass();
        combinerGenericTypes = superclasse.getActualTypeArguments();
    }


    /**
     * Définit la classe du reducer
     *
     * voir {@link org.apache.hadoop.mapreduce.Job#setReducerClass(Class<? extends Reducer> cls) setReducerClass}
     */
    public void setReducerClass(Class<? extends Reducer<? extends Writable,? extends Writable,? extends Writable,? extends Writable>> classe)
    {
        job.setReducerClass(classe);

        // obtenir les types génériques du reducer
        ParameterizedType superclasse = (ParameterizedType) classe.getGenericSuperclass();
        reducerGenericTypes = superclasse.getActualTypeArguments();
    }


    /**
     * Spécifie le format des fichiers à traiter en entrée : définit les clés et valeurs que reçoit le mapper
     *
     * voir {@link org.apache.hadoop.mapreduce.Job#setInputFormatClass(Class<? extends InputFormat> cls) setInputFormatClass}
     */
    public void setInputFormatClass(Class<? extends InputFormat<? extends Writable, ? extends Writable>> classe)
    {
        job.setInputFormatClass(classe);

        // obtenir les types génériques du format
        ParameterizedType superclasse = (ParameterizedType) classe.getGenericSuperclass();
        inputGenericTypes = superclasse.getActualTypeArguments();
    }


    /**
     * Spécifie le format des fichiers à traiter en entrée : définit les clés et valeurs que reçoit le mapper
     * variante pour des classes comme SequenceFileInputFormat qui ne permettent pas de connaître les types des clés et valeurs
     *
     * voir {@link org.apache.hadoop.mapreduce.Job#setInputFormatClass(Class<? extends InputFormat> cls) setInputFormatClass}
     */
    public void setInputFormatClass(Class<? extends InputFormat> classe, Class<? extends Writable> keyClass, Class<? extends Writable> valueClass)
    {
        job.setInputFormatClass(classe);

        // mémoriser les types génériques du format
        inputGenericTypes = new Type[] { keyClass, valueClass };
    }


    /**
     * Ajoute un dossier ou un fichier à traiter en entrée
     *
     * voir {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat#addInputPath(Job job, Path path) addInputPath}
     */
    public void addInputPath(Path path) throws IOException
    {
        FileInputFormat.addInputPath(job, path);
    }


    /**
     * Indique qu'il faut parcourir le dossier d'entrée de manière récursive
     *
     * voir {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat#setInputDirRecursive(Job job, boolean inputDirRecursive) setInputDirRecursive}
     */
    public void setInputDirRecursive(boolean inputDirRecursive)
    {
        FileInputFormat.setInputDirRecursive(job, inputDirRecursive);
    }


    /**
     * Spécifie le format de sortie du job : clés et valeurs produites par le reducer
     *
     * voir {@link org.apache.hadoop.mapreduce.Job#setOutputFormatClass(Class<? extends OutputFormat> cls) setOutputFormatClass}
     */
    @SuppressWarnings("rawtypes")
    public void setOutputFormatClass(Class<? extends OutputFormat> classe)
    {
        job.setOutputFormatClass(classe);

        // on ne peut pas vérifier les types paramètres génériques de la classe car ils dérivent d'Object
    }

    /**
     * Définit le dossier dans lesquels les résultats vont être écrits
     *
     * voir {@link org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#setOutputPath(Job job, Path outputDir) setOutputPath}
     */
    public void setOutputPath(Path path)
    {
        FileOutputFormat.setOutputPath(job, path);
    }


    /**
     * Cette méthode vérifie tout d'abord la structure du job : types des classes génériques et paramétrage
     * Ensuite, elle lance le job et attend sa fin.
     *
     * voir {@link org.apache.hadoop.mapreduce.Job#waitForCompletion(boolean verbose) waitForCompletion}
     */
    public boolean waitForCompletion(boolean verbose) throws Exception
    {
        // classes de base présentes ?
        if (inputGenericTypes == null) {
            System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
            System.err.println(ANSI_RED+"Il manque un appel à setInputFormatClass"+ANSI_RESET);
            throw new IllegalArgumentException("InputFormat non defini");
        }
        if (mapperGenericTypes == null) {
            System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
            System.err.println(ANSI_RED+"Il manque un appel à setMapperClass"+ANSI_RESET);
            throw new IllegalArgumentException("Mapper non defini");
        }
        if (reducerGenericTypes == null) {
            System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
            System.err.println(ANSI_RED+"Il manque un appel à setReducerClass"+ANSI_RESET);
            throw new IllegalArgumentException("Reducer non defini");
        }

        // vérifier le mapper par rapport au format d'entrée
        if (inputGenericTypes[0] != mapperGenericTypes[0]) {
            System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
            System.err.println(ANSI_RED+"InputFormat => K="+inputGenericTypes[0]+" V="+inputGenericTypes[1]+ANSI_RESET);
            System.err.println(ANSI_RED+"or Mapper<K="+mapperGenericTypes[0]+", V="+mapperGenericTypes[1]+", ...>"+ANSI_RESET);
            throw new IllegalArgumentException("Format d'entrée et Mapper incompatibles (types des clés)");
        }
        if (inputGenericTypes[1] != mapperGenericTypes[1]) {
            System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
            System.err.println(ANSI_RED+"InputFormat => K="+inputGenericTypes[0]+" V="+inputGenericTypes[1]+ANSI_RESET);
            System.err.println(ANSI_RED+"or Mapper<K="+mapperGenericTypes[0]+", V="+mapperGenericTypes[1]+", ...>"+ANSI_RESET);
            throw new IllegalArgumentException("Format d'entrée et Mapper incompatibles (types des valeurs)");
        }

        // vérifier le combiner
        if (combinerGenericTypes != null) {
            if (combinerGenericTypes[0] != combinerGenericTypes[2]) {
                System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
                System.err.println(ANSI_RED+"La classe combiner doit voir les clés du même type en entrée et sortie"+ANSI_RESET);
                throw new IllegalArgumentException("Clés du Combiner incorrectes");
            }
            if (combinerGenericTypes[1] != combinerGenericTypes[3]) {
                System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
                System.err.println(ANSI_RED+"La classe combiner doit voir les valeurs du même type en entrée et sortie"+ANSI_RESET);
                throw new IllegalArgumentException("Valeurs du Combiner incorrectes");
            }
            if (combinerGenericTypes[0] != mapperGenericTypes[2]) {
                System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
                System.err.println(ANSI_RED+"Mapper => K="+mapperGenericTypes[2]+" V="+mapperGenericTypes[3]+ANSI_RESET);
                System.err.println(ANSI_RED+"or Combiner<K="+combinerGenericTypes[0]+", V="+combinerGenericTypes[1]+", ...>"+ANSI_RESET);
                throw new IllegalArgumentException("Mapper et Combiner incompatibles (types des clés)");
            }
            if (combinerGenericTypes[1] != mapperGenericTypes[3]) {
                System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
                System.err.println(ANSI_RED+"Mapper => K="+mapperGenericTypes[2]+" V="+mapperGenericTypes[3]+ANSI_RESET);
                System.err.println(ANSI_RED+"or Combiner<K="+combinerGenericTypes[0]+", V="+combinerGenericTypes[1]+", ...>"+ANSI_RESET);
                throw new IllegalArgumentException("Mapper et Combiner incompatibles (types des valeurs)");
            }
            if (combinerGenericTypes[2] != reducerGenericTypes[0]) {
                System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
                System.err.println(ANSI_RED+"Combiner => K="+combinerGenericTypes[2]+" V="+combinerGenericTypes[3]+ANSI_RESET);
                System.err.println(ANSI_RED+"or Reducer<K="+reducerGenericTypes[0]+", V="+reducerGenericTypes[1]+", ...>"+ANSI_RESET);
                throw new IllegalArgumentException("Combiner et Reducer incompatibles (types des clés)");
            }
            if (combinerGenericTypes[3] != reducerGenericTypes[1]) {
                System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
                System.err.println(ANSI_RED+"Combiner => K="+combinerGenericTypes[2]+" V="+combinerGenericTypes[3]+ANSI_RESET);
                System.err.println(ANSI_RED+"or Reducer<K="+reducerGenericTypes[0]+", V="+reducerGenericTypes[1]+", ...>"+ANSI_RESET);
                throw new IllegalArgumentException("Combiner et Reducer incompatibles (types des valeurs)");
            }
        }

        // vérifier le reducer par rapport au mapper
        if (mapperGenericTypes[2] != reducerGenericTypes[0]) {
            System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
            System.err.println(ANSI_RED+"Mapper => K="+mapperGenericTypes[2]+" V="+mapperGenericTypes[3]+ANSI_RESET);
            System.err.println(ANSI_RED+"or Reducer<K="+reducerGenericTypes[0]+", V="+reducerGenericTypes[1]+", ...>"+ANSI_RESET);
            throw new IllegalArgumentException("Mapper et Reducer incompatibles (types des clés)");
        }
        if (mapperGenericTypes[3] != reducerGenericTypes[1]) {
            System.err.println(ANSI_RED_BOLD+"Il y a une erreur dans votre job MapReduce :"+ANSI_RESET);
            System.err.println(ANSI_RED+"Mapper => K="+mapperGenericTypes[2]+" V="+mapperGenericTypes[3]+ANSI_RESET);
            System.err.println(ANSI_RED+"or Reducer<K="+reducerGenericTypes[0]+", V="+reducerGenericTypes[1]+", ...>"+ANSI_RESET);
            throw new IllegalArgumentException("Mapper et Reducer incompatibles (types des valeurs)");
        }

        // finaliser la configuration du job
        job.setMapOutputKeyClass((Class<?>) mapperGenericTypes[2]);
        job.setMapOutputValueClass((Class<?>) mapperGenericTypes[3]);
        job.setOutputKeyClass((Class<?>) reducerGenericTypes[2]);
        job.setOutputValueClass((Class<?>) reducerGenericTypes[3]);

        // lancer le job
        return job.waitForCompletion(verbose);
    }
}
