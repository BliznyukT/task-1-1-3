package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import jm.task.core.jdbc.exception.UserDaoJDBCImplException;
import jm.task.core.jdbc.util.Util;

public class UserDaoJDBCImpl implements UserDao {
    private final static Logger LOGGER = Logger.getLogger(UserDaoJDBCImpl.class.getName());
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS users (id BIGINT PRIMARY KEY" +
            " AUTO_INCREMENT , name VARCHAR(50) NOT NULL , lastName VARCHAR(50) NOT NULL ,\n" +
            "    age TINYINT UNSIGNED NOT NULL CHECK ( age < 99))";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS users";
    private static final String CLEAN_TABLE = "TRUNCATE TABLE users";
    private static final String INSERT_USER = "INSERT INTO users (name, lastname, age) VALUES (?,?,?)";
    private static final String DELETE_USER = "DELETE FROM users WHERE id = ?";
    private static final String SELECT_ALL = "SELECT * FROM users";

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        executeStatement("Создана таблица users", CREATE_TABLE);
    }

    public void dropUsersTable() {
        executeStatement("Удалена таблица users", DROP_TABLE);
    }

    public void cleanUsersTable() {
        executeStatement("Удалены данные из таблицы users", CLEAN_TABLE);
    }


    public void saveUser(String name, String lastName, byte age) {
        try (Connection conn = Util.getConnection()) {
            try (PreparedStatement pstmt = conn.prepareStatement(INSERT_USER)) {
                pstmt.setString(1, name);
                pstmt.setString(2, lastName);
                pstmt.setByte(3, age);
                pstmt.executeUpdate();
                conn.commit();
                LOGGER.info(() -> "User с именем - " + name + " добавлен в базу данных");
            } catch (SQLException e) {
                rollbackTransaction(conn);
                customExceptionHandler(e, "Не удалось добавить в таблицу пользователя : " +
                        "name " + name + ", lastName " + lastName);
            }
        } catch (SQLException e) {
            customExceptionHandler(e, "Не удалось установить соединение с базой данных");
        }
        System.out.println("User с именем - " + name + " добавлен в базу данных");
    }

    public void removeUserById(long id) {
        try (Connection conn = Util.getConnection()) {
            try (PreparedStatement pstmt = conn.prepareStatement(DELETE_USER)) {
                pstmt.setLong(1, id);
                pstmt.executeUpdate();
                conn.commit();
                LOGGER.info(() -> "Пользователь c id = " + id + " удален из базы данных");
            } catch (SQLException e) {
                rollbackTransaction(conn);
                customExceptionHandler(e, "Не удалось удалить пользователя с id = "
                        + id + " из базы данных");
            }
        } catch (SQLException e) {
            customExceptionHandler(e, "Не удалось установить соединение с базой данных");
            throw new RuntimeException(e);
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (Connection conn = Util.getConnection()) {
            try (PreparedStatement pstmt = conn.prepareStatement(SELECT_ALL);
                 ResultSet resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    users.add(new User(resultSet.getLong("id"),
                            resultSet.getString("name"),
                            resultSet.getString("lastName"),
                            resultSet.getByte("age")));
                }
            } catch (SQLException e) {
                rollbackTransaction(conn);
                customExceptionHandler(e, "Не удалось получить список пользователей из users");
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            customExceptionHandler(e, "Не удалось установить соединение с базой данных");
        }
        return users;
    }

    private void executeStatement(String message, String action) {
        try (Connection conn = Util.getConnection()) {
            try (Statement statement = conn.createStatement()) {
                statement.execute(action);
                conn.commit();
                LOGGER.info(message);
            } catch (SQLException e) {
                rollbackTransaction(conn);
                customExceptionHandler(e, "Не удалось выполнить запрос -> " + action);
            }
        } catch (SQLException e) {
            customExceptionHandler(e, "Не удалось установить соединение с базой данных");
        }
    }

    private void rollbackTransaction(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
                LOGGER.warning("Транзакция отменена");
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, "Не удалось отменить транзакцию -> " + e.getMessage(), e);
            }
        }
    }

    private void customExceptionHandler(SQLException e, String exceptionMessage) {
        LOGGER.severe(e.getMessage() + " -> " + exceptionMessage);
        throw new UserDaoJDBCImplException(exceptionMessage, e);
    }
}