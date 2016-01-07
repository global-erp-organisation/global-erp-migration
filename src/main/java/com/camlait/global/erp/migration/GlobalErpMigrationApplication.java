package com.camlait.global.erp.migration;

import java.sql.SQLException;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.camlait.global.erp.domain.config.GlobalAppConstants;
import com.camlait.global.erp.domain.exception.GlobalErpServiceException;
import com.camlait.global.erp.migration.dao.MysqlConnection;
import com.camlait.global.erp.migration.service.MigrateData;
import com.camlait.global.erp.service.inventaire.IInventaireService;
import com.camlait.global.erp.service.organisation.ILocalisationService;
import com.camlait.global.erp.service.partenaire.IPartenaireService;
import com.camlait.global.erp.service.produit.IProduitService;

@SpringBootApplication
@EnableTransactionManagement
@EntityScan(GlobalAppConstants.DOMAIN_BASE_PACKAGE)
@EnableJpaRepositories(GlobalAppConstants.DAO_BASE_PACKAGE)
public class GlobalErpMigrationApplication {
    
    @Autowired
    private IProduitService produitService;
    
    @Autowired
    private ILocalisationService localService;
    
    @Autowired
    private IPartenaireService partenaireService;
    
    @Autowired
    private IInventaireService inventaire;
    
    public static void main(String[] args) {
        SpringApplication.run(GlobalErpMigrationApplication.class, args);
    }
    
    @PostConstruct
    public void start() throws GlobalErpServiceException, IllegalArgumentException, SQLException {
        MysqlConnection m =new MysqlConnection();
        MigrateData.produit(produitService, m);
        MigrateData.partenaire(partenaireService, localService, m);
        MigrateData.entrepot(inventaire, localService, m);
    }
    
}
