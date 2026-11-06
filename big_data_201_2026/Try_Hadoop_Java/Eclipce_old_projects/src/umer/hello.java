package umer;

public class hello {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	for( int i=1;i<=15;i++) {
		for( int j=1;j<=15;j++) {
			System.out.print("");
		}
		for( int k=1;k<=(i*2)-1;k++) {
			System.out.print("*");
		}
		System.out.println(" ");//maybe not
	}
	for( int i=1;i<=12;i++) {
		for( int j=1;j<=i;j++) {
			System.out.print(" ");
		}
		for( int k=1;k<=(30-(i*2-1));k++) {
		if (k==1|| k==(30-(i*2-1)))
			System.out.print("*");
		else
			System.out.print(" ");
		}
		System.out.println(" ");
	}
	for( int i=2;i<=5;i++) {
		for( int j=13;j>=i;j--) {
			System.out.print(" ");
		}
	    for( int k=1;k<=(i*2+1);k++) {
	    	System.out.print("*");
	}
	    System.out.println(" ");
	}
	System.out.println("RAMADAN KARIM");
	
	}
	
}
