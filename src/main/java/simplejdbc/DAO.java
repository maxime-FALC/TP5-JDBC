package simplejdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DAO {

	private final DataSource myDataSource;

	/**
	 *
	 * @param dataSource la source de données à utiliser
	 */
	public DAO(DataSource dataSource) {
		this.myDataSource = dataSource;
	}

	/**
	 *
	 * @return le nombre d'enregistrements dans la table CUSTOMER
	 * @throws DAOException
	 */
	public int numberOfCustomers() throws DAOException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM CUSTOMER";
		// Syntaxe "try with resources" 
		// cf. https://stackoverflow.com/questions/22671697/try-try-with-resources-and-connection-statement-and-resultset-closing
		try (Connection connection = myDataSource.getConnection(); // Ouvrir une connexion
			Statement stmt = connection.createStatement(); // On crée un statement pour exécuter une requête
			ResultSet rs = stmt.executeQuery(sql) // Un ResultSet pour parcourir les enregistrements du résultat
		) {
			rs.next(); // Pas la peine de faire while, il y a 1 seul enregistrement
			// On récupère le champ NUMBER de l'enregistrement courant
			result = rs.getInt("NUMBER");

		} catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}

		return result;
	}

	/**
	 * Detruire un enregistrement dans la table CUSTOMER
	 *
	 * @param customerId la clé du client à détruire
	 * @return le nombre d'enregistrements détruits (1 ou 0 si pas trouvé)
	 * @throws DAOException
	 */
	public int deleteCustomer(int customerId) throws DAOException {

		// Une requête SQL paramétrée
		String sql = "DELETE FROM CUSTOMER WHERE CUSTOMER_ID = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql))
                {
			// Définir la valeur du paramètre
			stmt.setInt(1, customerId);

			return stmt.executeUpdate();

		} catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}
	}

	/**
	 *
	 * @param customerId la clé du client à recherche
	 * @return le nombre de bons de commande pour ce client (table PURCHASE_ORDER)
	 * @throws DAOException
	 */
	public int numberOfOrdersForCustomer(int customerId) throws DAOException {
            /* nombre de commandes du client */
            int result = 0;
            
            // Une requête SQL paramétrée
            String sql = "SELECT COUNT (*) AS NUMBERORDERS FROM PURCHASE_ORDER "
                            + "WHERE CUSTOMER_ID = ?";
                
            // connexion à la BD puis execution de la requete    
            try (Connection connection = myDataSource.getConnection();
		PreparedStatement stmt = connection.prepareStatement(sql)) {
                
		// Définir la valeur du paramètre
		stmt.setInt(1, customerId);
                
                // récupération de la valeur renvoyée
                ResultSet rs = stmt.executeQuery();
                
                rs.next();
                
		result = rs.getInt("NUMBERORDERS");

            } catch (SQLException ex) {
		Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
		throw new DAOException(ex.getMessage());
            }
            
            return result;
	}

	/**
	 * Trouver un Customer à partir de sa clé
	 *
	 * @param customerID la clé du CUSTOMER à rechercher
	 * @return l'enregistrement correspondant dans la table CUSTOMER, ou null si pas trouvé
	 * @throws DAOException
	 */
	CustomerEntity findCustomer(int customerID) throws DAOException {
            
            // résultat de la requête
            CustomerEntity client;
            
            
            // Une requête SQL paramétrée
            String sql = "SELECT * FROM CUSTOMER "
                            + "WHERE CUSTOMER_ID = ?";
                
            // connexion à la BD puis execution de la requete    
            try (Connection connection = myDataSource.getConnection();
		PreparedStatement stmt = connection.prepareStatement(sql)) {
                
		// Définir la valeur du paramètre
		stmt.setInt(1, customerID);

                // récupération de la valeur renvoyée
                ResultSet rs = stmt.executeQuery();

                rs.next();
                
                client = new CustomerEntity(customerID, rs.getString("NAME"),
                                                rs.getString("ADDRESSLINE1"));
                
            } catch (SQLException ex) {
		Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
		throw new DAOException(ex.getMessage());
            }
            
            return client;
	}

	/**
	 * Liste des clients localisés dans un état des USA
	 *
	 * @param state l'état à rechercher (2 caractères)
	 * @return la liste des clients habitant dans cet état
	 * @throws DAOException
	 */
	List<CustomerEntity> customersInState(String state) throws DAOException {
            
            
            
            /* nombre de clients résidant dans l'état */
            List<CustomerEntity> result = new LinkedList<>() ;
            
            // chaque client devant être traité
            CustomerEntity client;
            
            // Une requête SQL paramétrée
            String sql = "SELECT * FROM CUSTOMER WHERE STATE = ?";
                
            // connexion à la BD puis execution de la requete    
            try (Connection connection = myDataSource.getConnection();
		PreparedStatement stmt = connection.prepareStatement(sql)) {
                
		// Définir la valeur du paramètre
		stmt.setString(1, state);
                
                // récupération de la valeur renvoyée
                ResultSet rs = stmt.executeQuery();
                
                while(rs.next()){
                    
                    client = new CustomerEntity(rs.getInt("CUSTOMER_ID"), rs.getString("NAME"),
                                                rs.getString("ADDRESSLINE1"));
                    
                    result.add(client);
                }
                

            } catch (SQLException ex) {
		Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
		throw new DAOException(ex.getMessage());
            }
            
            return result;
	}

}
