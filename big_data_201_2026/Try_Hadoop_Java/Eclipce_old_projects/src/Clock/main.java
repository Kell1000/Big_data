package Clock;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;

import javax.swing.JFrame;

public class main {
	
	static Shape circle = new Ellipse2D.Double(0, 0, 200, 200);

	public static void main(String[] args) {
		
		Window clock = new Window() {
			Graphics2D g2d;
			public void paint(Graphics g) {
				g2d = (Graphics2D)g;
				g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
				g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
				
				g2d.setColor(Color.GRAY);
				g2d.fillRect(0, 0, 200, 200);
				g2d.drawImage(img, 0, 0, 200, 200, this);
				
				g2d.setColor(Color.white);
				g2d.translate(100, 100);
				g2d.rotate(Math.toRadians(second*6));
				g2d.setStroke(thin);
				g2d.drawLine(0, 0, 0, -85);
				g2d.rotate(-Math.toRadians(second*6));
				g2d.translate(-100, -100);
				
				
				g2d.translate(100, 100);
				g2d.rotate(Math.toRadians(minute*6));
				g2d.setStroke(thin);
				g2d.drawLine(0, 0, 0, -70);
				g2d.rotate(-Math.toRadians(minute*6));
				g2d.translate(-100, -100);
				
				g2d.translate(100, 100);
				g2d.rotate(Math.toRadians(hour*30));
				g2d.setStroke(thin);
				g2d.drawLine(0, 0, 0, -50);
				g2d.rotate(-Math.toRadians(hour*30));
				g2d.translate(-100, -100);
			}
		};
		
		clock.setUndecorated(true);
		clock.setLocation(0, 0);
		clock.setSize(200, 200);
		clock.setShape(circle);
		clock.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		clock.setVisible(true);
}
}
