package com.rllerena;

import javax.swing.*;

public class Main {

    public static void main(String[] args) {
        JFrame jFrame=new JFrame();
        jFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        jFrame.setBounds(500,100,500,500);
//        jFrame.setBounds(30, 30, 200, 200);
        jFrame.getContentPane().add(new Gota());
        jFrame.setVisible(true);
    }
}
