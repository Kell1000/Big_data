package GUICoolness;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

public class GUI implements ActionListener {
	
	private static JLabel Userlabel;
	private static JLabel Passwordlabel;
	private static JTextField UserText;
	private static JPasswordField PasswordText;
	private static JButton button;
	private static JLabel success;
	
	private static JFrame frame;
	private static JPanel panel;

	public static void main(String[] args) {
				
			 frame= new JFrame();
			 panel = new JPanel();
				frame.setSize(300, 200);
				frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
				frame.add(panel);
				
				panel.setLayout(null);
			 Userlabel=new JLabel("User:");
		        Userlabel.setBounds(10, 20, 80, 25);
		        panel.add(Userlabel);
		        
			 UserText = new JTextField(20);
			    UserText.setBounds(100,20,165,25);
			    panel.add(UserText);
			    
			Passwordlabel=new JLabel("Password:");
		        Passwordlabel.setBounds(10, 60, 80, 25);
		        panel.add(Passwordlabel);
		        
			 PasswordText = new JPasswordField(20);
			    PasswordText.setBounds(100,60,165,25);
			    panel.add(PasswordText);
			    
			 button =new JButton("login");
		        button.setBounds(125, 110, 110, 25);
		        button.addActionListener(new GUI());
		        panel.add(button);
			success = new JLabel("");
			    success.setBounds(100, 140, 190, 30);
			    panel.add(success);
			    
			    frame.setVisible(true);
			    
			
					

		}

	@Override
	public void actionPerformed(ActionEvent e) {
		
		String user = UserText.getText();
		@SuppressWarnings("deprecation")
		String password = PasswordText.getText();
		if(user.equals("omar")&&password.equals("1998")) {
		    frame= new JFrame();
		    panel = new JPanel();
				frame.setSize(300, 200);
				frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
				frame.add(panel);
				
			
			    
			    /////////////////////////
			    frame= new JFrame();
				 panel = new JPanel();
					frame.setSize(300, 200);
					frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
					frame.add(panel);
					
					panel.setLayout(null);
			    Userlabel=new JLabel("User:");
		        Userlabel.setBounds(10, 20, 80, 25);
		        panel.add(Userlabel);
		        
			 UserText = new JTextField(20);
			    UserText.setBounds(100,20,165,25);
			    panel.add(UserText);
			    
			Passwordlabel=new JLabel("Password:");
		        Passwordlabel.setBounds(10, 60, 80, 25);
		        panel.add(Passwordlabel);
		        
			 PasswordText = new JPasswordField(20);
			    PasswordText.setBounds(100,60,165,25);
			    panel.add(PasswordText);
			    
			 button =new JButton("login");
		        button.setBounds(125, 110, 110, 25);
		        button.addActionListener(new GUI());
		        panel.add(button);
			success = new JLabel("");
			    success.setBounds(100, 140, 190, 30);
			    panel.add(success);
			    
			    frame.setVisible(true);
			    
			
					

			    ////////////////////////
			    
			    success = new JLabel("");
			    success.setBounds(100, 140, 190, 30);
			    panel.add(success);
			    success.setText("Welcome!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			    frame.setVisible(true);
			    
		}
			
		


		
	}

	}


