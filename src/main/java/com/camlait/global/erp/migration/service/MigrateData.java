package com.camlait.global.erp.migration.service;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;

import com.camlait.global.erp.dao.partenaire.ClientAMargeDao;
import com.camlait.global.erp.domain.enumeration.Portee;
import com.camlait.global.erp.domain.exception.GlobalErpServiceException;
import com.camlait.global.erp.domain.organisation.Centre;
import com.camlait.global.erp.domain.organisation.Region;
import com.camlait.global.erp.domain.organisation.Secteur;
import com.camlait.global.erp.domain.organisation.Zone;
import com.camlait.global.erp.domain.partenaire.Caissier;
import com.camlait.global.erp.domain.partenaire.Client;
import com.camlait.global.erp.domain.partenaire.ClientAmarge;
import com.camlait.global.erp.domain.partenaire.Emplois;
import com.camlait.global.erp.domain.partenaire.Employe;
import com.camlait.global.erp.domain.partenaire.Vendeur;
import com.camlait.global.erp.domain.produit.CategorieProduit;
import com.camlait.global.erp.domain.produit.Produit;
import com.camlait.global.erp.migration.dao.AbstractConnection;
import com.camlait.global.erp.service.organisation.ILocalisationService;
import com.camlait.global.erp.service.partenaire.IPartenaireService;
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
    
    public static void partenaire(IPartenaireService partenaireService,
            ILocalisationService localisationService,
            AbstractConnection m) throws SQLException {
        String sql = "";
        ResultSet r = null;
        
        // Emplois
        sql = "select CodeEmploi code, IntituleEmplois description from templois";
        r = m.execute(sql);
        while (r.next()) {
            Emplois e = new Emplois();
            e.setCodeEmplois(r.getString("code"));
            e.setDescriptionEmplois(r.getString("description"));
            partenaireService.ajouterEmplois(e);
        }
        
        // Organisation
        organisation(localisationService, m);
        
        // Vendeur
        sql = "select CodePersonnel matricule, NomPersonnel nom, PrenomPersonnel prenom, DateDenaissancePersonnel nais, TauxCommission tcom, Commission com, CodeZone zone, Key_Timmo veh, AdressePersonnel addr  from temploye where Key_Templois='VD'";
        r = m.execute(sql);
        while (r.next()) {
            Vendeur v = new Vendeur();
            v.setAdresse(r.getString("addr"));
            v.setDateDeNaissance(r.getDate("nais"));
            v.setEmplois(partenaireService.obtenirEmplois("VD"));
            v.setMatricule(r.getString("matricule"));
            v.setNom(r.getString("nom"));
            v.setPrenom(r.getString("prenom"));
            v.setRecoisDesCommission(r.getBoolean("com"));
            v.setTauxDeCommission(r.getDouble("tcom"));
            v.setZoneDeVente(localisationService.obtenirLocalisation(Zone.class, r.getString("zone")));
            partenaireService.ajouterPartenaire(v);
        }
        // Caissiers
        
        sql = "select CodePersonnel matricule, NomPersonnel nom, PrenomPersonnel prenom, DateDenaissancePersonnel nais, Key_Timmo veh, AdressePersonnel addr,Key_Templois emplois  from temploye where Key_Templois='CA'";
        r = m.execute(sql);
        while (r.next()) {
            Caissier v = new Caissier();
            v.setAdresse(r.getString("addr"));
            v.setDateDeNaissance(r.getDate("nais"));
            v.setEmplois(partenaireService.obtenirEmplois(r.getString("emplois")));
            v.setMatricule(r.getString("matricule"));
            v.setNom(r.getString("nom"));
            v.setPrenom(r.getString("prenom"));
            partenaireService.ajouterPartenaire(v);
        }
        
        // Autres employes
        
        sql = "select CodePersonnel matricule, NomPersonnel nom, PrenomPersonnel prenom, DateDenaissancePersonnel nais, Key_Timmo veh, AdressePersonnel addr,Key_Templois emplois  from temploye where Key_Templois not in ('VD','CA')";
        r = m.execute(sql);
        while (r.next()) {
            Employe v = new Employe();
            v.setAdresse(r.getString("addr"));
            v.setDateDeNaissance(r.getDate("nais"));
            v.setEmplois(partenaireService.obtenirEmplois(r.getString("emplois")));
            v.setMatricule(r.getString("matricule"));
            v.setNom(r.getString("nom"));
            v.setPrenom(r.getString("prenom"));
            partenaireService.ajouterPartenaire(v);
        }
        
        // Client Marge
        sql = "select IntitulePartenaire description, Key_Tpartenaire code, ClientaRistourne ristourne, ClientaMarge marge, Tauxristournes tris, Codezone zone, ClienFrigo frigo from tpartenairettypepartenaire where ClientaMarge=1";
        r = m.execute(sql);
        while (r.next()) {
            ClientAmarge c = new ClientAmarge();
            c.setClientAristourne(false);
            c.setCodePartenaire(r.getString("code"));
            c.setDescription(r.getString("description"));
            c.setZone(localisationService.obtenirLocalisation(Zone.class, r.getString("zone")));
            partenaireService.ajouterPartenaire(c);
        }
        
        // Autres client
        
        sql = "select IntitulePartenaire description, Key_Tpartenaire code, ClientaRistourne ristourne, ClientaMarge marge, Tauxristournes tris, Codezone zone, ClienFrigo frigo from tpartenairettypepartenaire where ClientaMarge=0";
        r = m.execute(sql);
        while (r.next()) {
            Client c = new Client();
            c.setClientAristourne(r.getBoolean("ristourne"));
            c.setRistourne(r.getDouble("tris"));
            c.setCodePartenaire(r.getString("code"));
            c.setDescription(r.getString("description"));
            c.setZone(localisationService.obtenirLocalisation(Zone.class, r.getString("zone")));
            partenaireService.ajouterPartenaire(c);
        }
        
    }
    
    private static void organisation(ILocalisationService localService, AbstractConnection m) throws SQLException {
        String sql = "";
        ResultSet r = null;
        
        // Centres
        sql = "select CodeCentre code, IntituleCentre description from tcentre";
        r = m.execute(sql);
        while (r.next()) {
            Centre c = new Centre();
            c.setCode(r.getString("code"));
            c.setDescriptionLocal(r.getString("description"));
            localService.ajouterLocalisation(c);
        }
        
        sql = "select Key_tregion code, IntituleRegion description, CodeCentre centre from tregion where Key_tregion<>'CBASSA'";
        r = m.execute(sql);
        while (r.next()) {
            Region re = new Region();
            re.setCode(r.getString("code"));
            re.setDescriptionLocal(r.getString("description"));
            re.setCentre(localService.obtenirLocalisation(Centre.class, r.getString("centre")));
            localService.ajouterLocalisation(re);
        }
        
        sql = "select CodeSecteur code, IntituleSecteur description, Key_Tregion region from tsecteur where  CodeSecteur<>'D1'";
        r = m.execute(sql);
        while (r.next()) {
            Secteur s = new Secteur();
            s.setCode(r.getString("code"));
            s.setDescriptionLocal(r.getString("description"));
            s.setRegion(localService.obtenirLocalisation(Region.class, r.getString("region")));
            localService.ajouterLocalisation(s);
        }
        
        sql = "select CodeCentre code, IntituleZone description, CodeSecteur secteur from tzone";
        r = m.execute(sql);
        while (r.next()) {
            Zone z = new Zone();
            z.setCode(r.getString("code"));
            z.setDescriptionLocal(r.getString("description"));
            z.setSecteur(localService.obtenirLocalisation(Secteur.class, r.getString("secteur")));
            localService.ajouterLocalisation(z);
        }
    }
}
