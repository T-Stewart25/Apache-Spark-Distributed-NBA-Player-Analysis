package org.example;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class ConfigWindow extends JFrame {
    private Config config;

    public ConfigWindow(Config config) {
        super("Configuration");
        this.config = config;

        setSize(400, 300);
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setLayout(null);

        JLabel computerNameLabel = new JLabel("Computer Name:");
        computerNameLabel.setBounds(20, 20, 150, 20);
        add(computerNameLabel);

        JTextField computerNameField = new JTextField(config.getComputerName());
        computerNameField.setBounds(180, 20, 200, 25);
        add(computerNameField);

        JLabel portLabel = new JLabel("Port:");
        portLabel.setBounds(20, 60, 150, 20);
        add(portLabel);

        JTextField portField = new JTextField(String.valueOf(config.getPort()));
        portField.setBounds(180, 60, 200, 25);
        add(portField);

        JLabel sqlitePathLabel = new JLabel("SQLite File Path:");
        sqlitePathLabel.setBounds(20, 100, 150, 20);
        add(sqlitePathLabel);

        JTextField sqlitePathField = new JTextField(config.getSqliteFilePath());
        sqlitePathField.setBounds(180, 100, 200, 25);
        add(sqlitePathField);

        JLabel csvPathLabel = new JLabel("CSV File Path:");
        csvPathLabel.setBounds(20, 140, 150, 20);
        add(csvPathLabel);

        JTextField csvPathField = new JTextField(config.getCsvFilePath());
        csvPathField.setBounds(180, 140, 200, 25);
        add(csvPathField);

        JButton saveButton = new JButton("Save");
        saveButton.setBounds(150, 200, 100, 30);
        add(saveButton);

        saveButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                config.setComputerName(computerNameField.getText());
                config.setPort(Integer.parseInt(portField.getText()));
                config.setSqliteFilePath(sqlitePathField.getText());
                config.setCsvFilePath(csvPathField.getText());
                JOptionPane.showMessageDialog(ConfigWindow.this, "Configuration saved.");
                dispose();
            }
        });
    }
}
