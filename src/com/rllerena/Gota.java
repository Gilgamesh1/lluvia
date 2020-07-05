package com.rllerena;


import javax.swing.*;
import java.awt.*;
import java.awt.geom.Line2D;
import java.util.Random;

public class Gota extends JComponent {
    private Thread anima;
//    private int x1;
//    private int y1;
    int radio=10;     	//radio de la pelota
    int x, y;       	//posición del centro de la pelota
    int dx = 100;     	//desplazamientos
    int dy = 100;
    int anchoApplet;
    int altoApplet;

    public void start() {
        if (anima == null) {
            anima = new Thread();
            anima.start();
        }
    }

    public void stop() {
        if (anima != null) {
            anima.stop();
            anima = null;
        }
    }

    public void run() {
        while (true) {
            mover();
        }
    }

    void mover() {
        x += dx;
        y += dy;
        if (x >= (anchoApplet - radio) || x <= radio) dx *= -1;
        if (y >= (altoApplet - radio) || y <= radio) dy *= -1;
        repaint();      //llama a update
    }

    public void paint(Graphics g) {
        g.setColor(Color.red);
        g.fillOval(x - radio, y - radio, 2 * radio, 2 * radio);
    }

   /* public void paint(Graphics g) {
        System.out.println("getWidth() {}" + getWidth());
        System.out.println("getHeight() {}" + getHeight());
        x1 = getWidth() / 2;
        y1 = 90;
        g.setColor(Color.magenta);

        g.drawLine(x1, y1, x1, y1 + 10);

    }*/

}
