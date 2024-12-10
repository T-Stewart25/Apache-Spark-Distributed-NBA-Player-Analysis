package org.example;

import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.*;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.LookupPaintScale;
import org.jfree.chart.renderer.xy.XYBlockRenderer;
import org.jfree.chart.title.PaintScaleLegend;
import org.jfree.data.xy.DefaultXYZDataset;
import org.jfree.data.xy.XYZDataset;
import org.jfree.chart.ui.RectangleEdge;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class VisualizationWindow extends JFrame {

    private double minZ = Double.MAX_VALUE;
    private double maxZ = Double.MIN_VALUE;

    public VisualizationWindow(String visualizationType, String hdfsOutputPath) {
        super("JFreeChart Heatmap Visualization");

        // Create dataset from HDFS
        XYZDataset dataset = createDatasetFromHDFS(hdfsOutputPath, visualizationType);

        if (dataset == null) {
            JOptionPane.showMessageDialog(this, "No data found for visualization.", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        // Create the heatmap chart
        final JFreeChart chart = createChart(dataset);
        setContentPane(new ChartPanel(chart));

        setSize(800, 600);
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
    }

    private JFreeChart createChart(XYZDataset dataset) {
        // X-Axis for "Game Context"
        NumberAxis xAxis = new NumberAxis("Game Context");
        xAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        xAxis.setLowerMargin(0);
        xAxis.setUpperMargin(0);

        // Y-Axis for "Z-Score-Dependent Height"
        NumberAxis yAxis = new NumberAxis("Performance Z-Score");
        yAxis.setStandardTickUnits(NumberAxis.createStandardTickUnits());
        yAxis.setLowerMargin(0);
        yAxis.setUpperMargin(0);

        // Paint scale dynamically based on min and max Z values
        LookupPaintScale paintScale = new LookupPaintScale(minZ, maxZ, Color.BLUE); 
        paintScale.add(minZ, Color.BLUE);
        paintScale.add((minZ + maxZ) / 2, Color.GREEN);
        paintScale.add(maxZ, Color.RED); 

        // Paint scale legend with explicit range
        NumberAxis scaleAxis = new NumberAxis("Z-Score");
        scaleAxis.setRange(minZ, maxZ); 
        PaintScaleLegend legend = new PaintScaleLegend(paintScale, scaleAxis);
        legend.setPosition(RectangleEdge.RIGHT);
        legend.setAxisLocation(AxisLocation.TOP_OR_RIGHT);
        legend.setMargin(50.0, 20.0, 80.0, 0.0);

        // Renderer and plot
        XYBlockRenderer renderer = new XYBlockRenderer();
        renderer.setPaintScale(paintScale);

        XYPlot plot = new XYPlot(dataset, xAxis, yAxis, renderer);

        JFreeChart chart = new JFreeChart("Heatmap Visualization", JFreeChart.DEFAULT_TITLE_FONT, plot, false);
        chart.addSubtitle(legend);

        return chart;
    }

    private XYZDataset createDatasetFromHDFS(String hdfsOutputPath, String visualizationType) {
        try {
            Process fetchOutput = Runtime.getRuntime().exec(new String[]{"hdfs", "dfs", "-cat", hdfsOutputPath + "/part-*"});
            BufferedReader outputReader = new BufferedReader(new InputStreamReader(fetchOutput.getInputStream()));

            List<Double> xValues = new ArrayList<>();
            List<Double> yValues = new ArrayList<>();
            List<Double> zValues = new ArrayList<>();

            String line;
            while ((line = outputReader.readLine()) != null) {
                String[] parts = line.split(",", 2);
                if (parts.length == 2 && parts[0].trim().equals(visualizationType)) {
                    String[] pairs = parts[1].split("\\)\\(");
                    for (String pair : pairs) {
                        pair = pair.replace("(", "").replace(")", "").trim();
                        String[] valueParts = pair.split(":");
                        if (valueParts.length == 2) {
                            try {
                                double x;
                                double z = Double.parseDouble(valueParts[1].trim());

                                
                                if (visualizationType.equals("Home/Away")) {
                                    String keyString = valueParts[0].trim().toLowerCase();
                                    if (keyString.equals("home")) {
                                        x = 1.0;
                                    } else if (keyString.equals("away")) {
                                        x = 0.0;
                                    } else if (keyString.equals("unknown")) {
                                        x = 2.0;
                                    } else {
                                        throw new NumberFormatException("Invalid Home/Away key: " + keyString);
                                    }
                                } else {
                                    x = Double.parseDouble(valueParts[0].trim());
                                }

                                xValues.add(x);
                                yValues.add(Math.abs(z)); 
                                zValues.add(z);


                                minZ = Math.min(minZ, z);
                                maxZ = Math.max(maxZ, z);
                            } catch (NumberFormatException ex) {
                                System.err.println("Skipping invalid value: " + pair);
                            }
                        }
                    }
                }
            }

            if (xValues.isEmpty() || yValues.isEmpty() || zValues.isEmpty()) {
                return null;
            }

            double[] xArray = xValues.stream().mapToDouble(Double::doubleValue).toArray();
            double[] yArray = yValues.stream().mapToDouble(Double::doubleValue).toArray();
            double[] zArray = zValues.stream().mapToDouble(Double::doubleValue).toArray();

            DefaultXYZDataset dataset = new DefaultXYZDataset();
            dataset.addSeries("Heatmap Data", new double[][]{xArray, yArray, zArray});
            return dataset;

        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        VisualizationWindow demo = new VisualizationWindow("Home/Away", "");
        demo.pack();
        demo.setVisible(true);
    }
}
