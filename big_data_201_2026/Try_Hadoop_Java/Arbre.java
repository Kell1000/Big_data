

public abstract class Arbre {

	static String[] champs;

	public static void fromLine(String ligne) {
		champs = ligne.split(";");
	}

	public static double getAnnee() throws NumberFormatException {
		try {
			return Double.parseDouble(champs[5]);
		}catch (Exception e) {
			return 0.0;
		}
		 
	}
	public static String getGenre() throws NumberFormatException {
		try {
			return champs[2];
		}catch (Exception e) {
			return "Autres";
		}
		 
	}
	public static double getHauteur() throws NumberFormatException {
		try {
			return Double.parseDouble(champs[6]);
		}catch (Exception e) {
			return 0.0;
		}
		 
	}

}
