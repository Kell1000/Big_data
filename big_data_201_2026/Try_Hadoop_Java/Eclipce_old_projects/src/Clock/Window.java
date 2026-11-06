package Clock;

import java.awt.BasicStroke;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionAdapter;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import javax.swing.JFrame;

public class Window extends JFrame{


	int x,y,holdX,holdY,hour,minute,second;
	Toolkit t = Toolkit.getDefaultToolkit();
	Image img = t.getImage("src/JavaWallClock.PNG");
	Date date;
	Timer tm = new Timer();
	BasicStroke thin = new BasicStroke(2),medium = new BasicStroke(4),thick = new BasicStroke(6);
	public Window() {
		tm.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				getTimer();
				repaint(0,0,200,200);
			}
		}, 1, 1000);
		
		addMouseListener(new MouseAdapter() {
			public void mousePressed(MouseEvent e) {
				holdX=e.getX();
				holdY=e.getY();
			}
		});
		
		addMouseMotionListener(new MouseMotionAdapter() {
			public void mouseDragged(MouseEvent e) {
				x = e.getXOnScreen()-holdX;
				y = e.getYOnScreen()-holdY;
				
				setLocation(x,y);
				
			}
		});
	}
	public void getTimer() {
		date = new Date();
		hour = date.getHours()-12;
		minute = date.getMinutes();
		second = date.getSeconds();
		//System.out.println(hour+"-"+minute+"-"+second);
	}
}
