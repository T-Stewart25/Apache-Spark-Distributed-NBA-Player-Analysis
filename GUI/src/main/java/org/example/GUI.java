package org.example;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.spark.launcher.SparkLauncher;

public class GUI {
    private static Config config = new Config();
    private static String hdfsOutputPath = "/output/spark-job-results";
    public static int playerId = -1;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Error: Spark Job JAR path or Spark home not provided.");
            System.exit(1);
        }

        String jarPath = args[0];
        String sparkHome = args[1]; 

        // Create a JFrame
        JFrame frame = new JFrame("Player and Team Input");
        frame.setSize(700, 400);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setLayout(null);

        // Config Button
        JButton configButton = new JButton("Config");
        configButton.setBounds(600, 10, 80, 30);
        frame.add(configButton);

        configButton.addActionListener(e -> SwingUtilities.invokeLater(() -> new ConfigWindow(config).setVisible(true)));

        JProgressBar progressBar = new JProgressBar();
        progressBar.setBounds(0, 350, 700, 20);
        progressBar.setVisible(false);
        frame.add(progressBar);

        // Player name input
        JLabel playerLabel = new JLabel("Enter a Player Name:");
        playerLabel.setBounds(50, 20, 200, 20);
        frame.add(playerLabel);

        JTextField playerField = new JTextField();
        playerField.setBounds(250, 20, 300, 30);
        frame.add(playerField);

        // Team name input
        JLabel teamLabel = new JLabel("Enter most recent team:");
        teamLabel.setBounds(50, 70, 200, 20);
        frame.add(teamLabel);

        JTextField teamField = new JTextField();
        teamField.setBounds(250, 70, 300, 30);
        frame.add(teamField);

        // Submit button
        JButton submitButton = new JButton("Submit");
        submitButton.setBounds(250, 120, 100, 30);
        frame.add(submitButton);

        JLabel resultLabel = new JLabel();
        resultLabel.setBounds(50, 170, 500, 30);
        frame.add(resultLabel);

        // SQL testing button
        JButton sqlButton = new JButton("Run Job");
        sqlButton.setBounds(250, 200, 100, 30);
        frame.add(sqlButton);

        // Create a button for visualization
        JButton visualizeButton = new JButton("Create Visualization");
        visualizeButton.setBounds(250, 300, 200, 30); 
        visualizeButton.setVisible(false);
        frame.add(visualizeButton);

        String[] visualizationOptions = {"Score", "Period", "Home/Away"};
        JComboBox<String> visualizationDropdown = new JComboBox<>(visualizationOptions);

        // Position the dropdown next to the button
        visualizationDropdown.setBounds(
                visualizeButton.getX() + visualizeButton.getWidth() + 10, 
                visualizeButton.getY(),                                 
                150,                                                     
                30                                                      
        );
        visualizationDropdown.setVisible(false);
        frame.add(visualizationDropdown);

        JLabel sqlResultLabel = new JLabel();
        sqlResultLabel.setBounds(50, 250, 500, 30);
        frame.add(sqlResultLabel);

        // Player and team submission button action
        submitButton.addActionListener(e -> {
            String playerName = playerField.getText();
            String teamName = teamField.getText();

            String url = "jdbc:sqlite:" + config.getSqliteFilePath();

            String sql = "SELECT p.id AS player_id " +
                    "FROM player p " +
                    "JOIN common_player_info cpi ON p.id = cpi.person_id " +
                    "JOIN team t ON cpi.team_id = t.id " +
                    "WHERE p.full_name COLLATE NOCASE = ? " +
                    "AND (t.nickname COLLATE NOCASE = ? OR t.full_name COLLATE NOCASE = ?)";

            try (Connection conn = DriverManager.getConnection(url);
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setString(1, playerName);
                pstmt.setString(2, teamName);
                pstmt.setString(3, teamName);

                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        playerId = rs.getInt("player_id");
                        resultLabel.setText("Player ID: " + playerId);
                    } else {
                        String fallbackSql = "SELECT id AS player_id FROM player WHERE full_name COLLATE NOCASE = ?";
                        try (PreparedStatement fallbackPstmt = conn.prepareStatement(fallbackSql)) {
                            fallbackPstmt.setString(1, playerName);

                            try (ResultSet fallbackRs = fallbackPstmt.executeQuery()) {
                                if (fallbackRs.next()) {
                                    playerId = fallbackRs.getInt("player_id");
                                    resultLabel.setText("Player ID (Name-Only): " + playerId);
                                } else {
                                    resultLabel.setText("No results found for the given name and team.");
                                }
                            }
                        }
                    }
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
                resultLabel.setText("Error: " + ex.getMessage());
            }
        });

        sqlButton.addActionListener(e -> {
            if (playerId == -1) {
                sqlResultLabel.setText("No Player ID available. Submit valid inputs first.");
                return;
            }

            final String csvPath = config.getCsvFilePath();
            final String masterUrl = "spark://" + config.getComputerName() + ":" + config.getPort();

            // Show progress bar and disable the button
            progressBar.setVisible(true);
            progressBar.setIndeterminate(true);
            sqlButton.setEnabled(false);

            // Run the Spark job in a separate thread
            new Thread(() -> {
                try {
                    // Delete HDFS output path if it exists
                    final Process deleteProcess = Runtime.getRuntime().exec(
                            new String[]{"hdfs", "dfs", "-rm", "-r", hdfsOutputPath});
                    deleteProcess.waitFor();

                    // Run Spark job
                    final Process sparkJob = new SparkLauncher()
                            .setAppResource(jarPath)
                            .setMainClass("org.example.IDReduce")
                            .setMaster(masterUrl)
                            .addAppArgs(csvPath, String.valueOf(playerId), hdfsOutputPath)
                            .setSparkHome(sparkHome)
                            .launch();

                    // Monitor Spark job progress 
                    Thread stdoutThread = new Thread(() -> {
                        try (BufferedReader reader = new BufferedReader(
                                new InputStreamReader(sparkJob.getInputStream()))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                final String finalLine = line;
                                SwingUtilities.invokeLater(() -> sqlResultLabel.setText(finalLine)); // Update the GUI
                            }
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    });

                    // Monitor Spark job errors 
                    Thread stderrThread = new Thread(() -> {
                        try (BufferedReader reader = new BufferedReader(
                                new InputStreamReader(sparkJob.getErrorStream()))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                System.err.println(line);
                            }
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    });

                    stdoutThread.start();
                    stderrThread.start();

                    // Wait for job completion
                    int exitCode = sparkJob.waitFor();
                    stdoutThread.join();
                    stderrThread.join();

                    if (exitCode == 0) {
                        SwingUtilities.invokeLater(() -> {
                            sqlResultLabel.setText("Spark job completed successfully.");
                            visualizeButton.setVisible(true);
                            visualizationDropdown.setVisible(true);
                        });
                    } else {
                        SwingUtilities.invokeLater(() -> sqlResultLabel.setText("Spark job failed with exit code: " + exitCode));
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                    SwingUtilities.invokeLater(() -> sqlResultLabel.setText("Error running Spark job: " + ex.getMessage()));
                } finally {
                    // Hide progress bar and enable the button
                    SwingUtilities.invokeLater(() -> {
                        progressBar.setVisible(false);
                        sqlButton.setEnabled(true);
                    });
                }
            }).start();
        });

        // Visualization button action
        visualizeButton.addActionListener(e -> {
            String selectedVisualization = (String) visualizationDropdown.getSelectedItem();
            SwingUtilities.invokeLater(() -> new VisualizationWindow(selectedVisualization, "/output/heatmap_data/").setVisible(true));
        });

        frame.setVisible(true);
    }
}
