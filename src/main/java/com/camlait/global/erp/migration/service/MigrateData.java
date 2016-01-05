package com.camlait.global.erp.migration.service;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;

import com.camlait.global.erp.domain.enumeration.Portee;
import com.camlait.global.erp.domain.exception.GlobalErpServiceException;
import com.camlait.global.erp.domain.produit.CategorieProduit;
import com.camlait.global.erp.domain.produit.Produit;
import com.camlait.global.erp.migration.dao.AbstractConnection;
import com.camlait.global.erp.service.produit.IProduitService;

public class MigrateData {
    
    private final static String TO_REMOVE = "CDOG";
    private final static String TO_UPDATE = "FROM";
    
    public static void produit(IProduitService produitService, AbstractConnection m)
            throws GlobalErpServiceException, IllegalArgumentException, SQLException {
            
        String sql = "select distinct CodeCategorieProduit code, IntituleCategorieProduit description from tcategorieproduit";
        
        ResultSet r = m.execute(sql);
        while (r.next()) {
            CategorieProduit c = new CategorieProduit();
            c.setCategorieTaxable(true);
            c.setCodeCategorieProduit(r.getString("code"));
            c.setDescriptionCategorie(r.getString("description"));
            c.setPortee(Portee.TOTAL);
            c.setSuiviEnStock(true);
            produitService.ajouterCategorieProduit(c);
        }
        CategorieProduit cp = produitService.obtenirCategorieProduit(TO_UPDATE);
        cp.setPortee(Portee.DETAIL);
        produitService.modifierCategorieProduit(cp);
        
        sql = "select CodeFamilleProduit code, IntituleFamilleProduit description, Key_Tcategorieproduit parent from tfamilleproduit where CodeFamilleProduit<>Key_Tcategorieproduit";
        
        r = m.execute(sql);
        while (r.next()) {
            CategorieProduit c = new CategorieProduit();
            c.setCategorieTaxable(true);
            c.setCodeCategorieProduit(r.getString("code"));
            c.setDescriptionCategorie(r.getString("description"));
            c.setPortee(Portee.DETAIL);
            c.setCategorieParent(produitService.obtenirCategorieProduit(r.getString("parent")));
            c.setSuiviEnStock(true);
            produitService.ajouterCategorieProduit(c);
        }
        
        sql = "select codeProduit code, IntituleProduit description, PrixUnitaireHorsTaxe pu, SuivieEnStock stock, ProduitTaxable taxable, Key_TfamilleProduit famille, PrixAmarge pm  from tproduit";
        r = m.execute(sql);
        while (r.next()) {
            Produit p = new Produit();
            String c = r.getString("famille");
            if (c.contains(TO_REMOVE))
                c = StringUtils.removeEnd(c, TO_REMOVE);
            p.setCategorie(produitService.obtenirCategorieProduit(c));
            p.setCodeProduit(r.getString("code"));
            p.setDescriptionProduit(r.getString("description"));
            p.setPrixUnitaireMarge(r.getDouble("pm"));
            p.setPrixUnitaireProduit(r.getDouble("pu"));
            p.setProduitTaxable(r.getBoolean("taxable"));
            p.setSuiviEnStock(r.getBoolean("stock"));
            produitService.ajouterProduit(p);
        }
    }
}
