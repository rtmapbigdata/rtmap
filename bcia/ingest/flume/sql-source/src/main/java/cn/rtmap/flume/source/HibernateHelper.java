package cn.rtmap.flume.source;

import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to manage hibernate sessions and perform queries
 * 
 *
 */
public class HibernateHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HibernateHelper.class);

    private static SessionFactory factory;
    private Session session;
    private ServiceRegistry serviceRegistry;
    private Configuration config;
    private SQLSourceHelper sqlSourceHelper;

    /**
     * Constructor to initialize hibernate configuration parameters
     * @param sqlSourceHelper Contains the configuration parameters from flume config file
     */
    public HibernateHelper(SQLSourceHelper sqlSourceHelper) {
        this.sqlSourceHelper = sqlSourceHelper;

        config = new Configuration()
                .setProperty("hibernate.connection.url", sqlSourceHelper.getConnectionURL())
                .setProperty("hibernate.connection.username", sqlSourceHelper.getUser())
                .setProperty("hibernate.connection.password", sqlSourceHelper.getPassword());

        if (sqlSourceHelper.getHibernateDialect() != null)
            config.setProperty("hibernate.dialect", sqlSourceHelper.getHibernateDialect());
        if (sqlSourceHelper.getHibernateDriver() != null)
            config.setProperty("hibernate.connection.driver_class", sqlSourceHelper.getHibernateDriver());
    }

    /**
     * Connect to database using hibernate
     */
    public void establishSession() {
        LOG.info("Opening hibernate session");

        serviceRegistry = new StandardServiceRegistryBuilder()
                .applySettings(config.getProperties()).build();
        factory = config.buildSessionFactory(serviceRegistry);
        session = factory.openSession();
    }

    /**
     * Close database connection
     */
    public void closeSession() {
        LOG.info("Closing hibernate session");

        session.close();
        factory.close();
    }

    /**
     * Execute the selection query in the database
     * @return The query result. Each Object is a cell content. <p>
     * The cell contents use database types (date,int,string...), 
     * keep in mind in case of future conversions/castings.
     */
    @SuppressWarnings("unchecked")
    public List<List<Object>> executeQuery() {
        List<List<Object>> rowsList = session.createSQLQuery(sqlSourceHelper.getQuery()).setResultTransformer(Transformers.TO_LIST).list();
        return rowsList;
    }

    @SuppressWarnings("unchecked")
    public String GetLastRowIndex() {
        // LOG.info("debug: " + sqlSourceHelper.getIndexQuery());
        List list = session.createSQLQuery(sqlSourceHelper.getIndexQuery()).setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP).list();
        if (list != null && list.size() > 0) {
            Map map = (Map)list.get(0);
            return map.get(sqlSourceHelper.getIndexColumn()).toString();
        } else 
            return null;
    }

    public void resetConnectionAndSleep() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        session.close();
        factory.close();
        establishSession();

        long execTime = System.currentTimeMillis() - startTime;

        if (execTime < sqlSourceHelper.getRunQueryDelay())
            Thread.sleep(sqlSourceHelper.getRunQueryDelay() - execTime);
    }
}
