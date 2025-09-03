package jm.task.core.jdbc.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;

public class Util {
    private static final Properties prop = new Properties();
    private static final String PROPERTIES_FILE = "db.properties";
    private static final Logger LOGGER = Logger.getLogger(Util.class.getName());

    static {
        try (InputStream in = Util.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            if (in == null) {
                throw new FileNotFoundException("Не удалось найти файл " + PROPERTIES_FILE);
            }
            prop.load(in);
            checkDBPropertiesParameters("db.url");
            checkDBPropertiesParameters("db.username");
            checkDBPropertiesParameters("db.password");

        } catch (FileNotFoundException e) {
            LOGGER.severe("Не найден файл с настройками для доступа к базе данных");
            throw new IllegalStateException("Не удалось найти конфигурацию для " +
                    "подключения к баз данных", e);
        } catch (IOException e) {
            LOGGER.severe("Не удалось загрузить настройки для доступа к базе данных " + e.getMessage());
            throw new IllegalStateException("Некорректная конфигурация для подключения к базе данных", e);
        }
    }

    public static Connection getConnection() {
        try {
            final Connection conn = DriverManager.getConnection(prop.getProperty("db.url"),
                    prop.getProperty("db.username"),
                    prop.getProperty("db.password"));
            conn.setAutoCommit(false);
            return conn;
        } catch (SQLException e) {
            LOGGER.severe("Не удалось установить соединение с базой данных" +
                    "проверьте пароль, логин, url");
            throw new IllegalStateException("Ошибка при подключении к базе данных");
        }
    }

    private static void checkDBPropertiesParameters(String parameter) {
        if (prop.getProperty(parameter) == null || prop.getProperty(parameter).isEmpty()) {
            throw new IllegalStateException("В конфигурационном файле отсутствует нужный параметр " + parameter);
        }
    }
}