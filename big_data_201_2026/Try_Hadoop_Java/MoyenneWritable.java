import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MoyenneWritable implements Writable {
	private double total = 0.0;
	private long nombre = 0L;

	public MoyenneWritable() {
		total = 0.0;
		nombre = 0L;
	}

	public MoyenneWritable(double valeur) {
		total = valeur;
		nombre = 1L;
	}

	public void write(DataOutput sortie) throws IOException {
		sortie.writeDouble(total);
		sortie.writeLong(nombre);
	}

	public void readFields(DataInput entree) throws IOException {
		total = entree.readDouble();
		nombre = entree.readLong();
	}

	public void set(double valeur) {
		total = valeur;
		nombre = 1L;
	}

	public void add(MoyenneWritable autre) {
		total += autre.total;
		nombre += autre.nombre;
	}

	public double getMoyenne() {
		return total / nombre;
	}

	public String toString() {
		return "Moyenne(total=" + total + ", nombre=" + nombre + ")";
	}
}
