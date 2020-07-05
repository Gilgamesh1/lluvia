package com.rllerena;

import java.awt.*;
import javax.swing.*;
import java.awt.geom.Line2D;

public class MyCanvas extends JComponent {

    public void paint(Graphics g) {

        // draw and display the line
        g.drawLine(30, 20, 80, 90);
    }
}
