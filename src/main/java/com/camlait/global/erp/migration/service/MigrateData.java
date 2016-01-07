package com.camlait.global.erp.migration.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.camlait.global.erp.domain.entrepot.Entrepot;
import com.camlait.global.erp.domain.entrepot.MagasinFixe;
import com.camlait.global.erp.domain.entrepot.MagasinMobile;
import com.camlait.global.erp.domain.enumeration.Portee;
import com.camlait.global.erp.domain.enumeration.Sexe;
import com.camlait.global.erp.domain.exception.GlobalErpServiceException;
import com.camlait.global.erp.domain.immobilisation.PartenaireImmobilisation;
import com.camlait.global.erp.domain.immobilisation.Refrigerateur;
import com.camlait.global.erp.domain.immobilisation.Vehicule;
import com.camlait.global.erp.domain.organisation.Centre;
import com.camlait.global.erp.domain.organisation.Localisation;
import com.camlait.global.erp.domain.organisation.Region;
import com.camlait.global.erp.domain.organisation.Secteur;
import com.camlait.global.erp.domain.organisation.Zone;
import com.camlait.global.erp.domain.partenaire.Caissier;
import com.camlait.global.erp.domain.partenaire.Client;
import com.camlait.global.erp.domain.partenaire.ClientAmarge;
import com.camlait.global.erp.domain.partenaire.Emplois;
import com.camlait.global.erp.domain.partenaire.Employe;
import com.camlait.global.erp.domain.partenaire.Magasinier;
import com.camlait.global.erp.domain.partenaire.Vendeur;
import com.camlait.global.erp.domain.produit.CategorieProduit;
import com.camlait.global.erp.domain.produit.Produit;
import com.camlait.global.erp.migration.dao.AbstractConnection;
import com.camlait.global.erp.service.inventaire.IInventaireService;
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

	public static void partenaire(IPartenaireService partenaireService, ILocalisationService localisationService,
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
		sql = "select CodePersonnel matricule, NomPersonnel nom, PrenomPersonnel prenom, DateDenaissancePersonnel nais, TauxCommission tcom, Commission com, CodeZone zone, Key_Timmo veh, AdressePersonnel addr, sexePersonnel sexe  from temploye where Key_Templois in ('VD','AD')";
		r = m.execute(sql);
		while (r.next()) {
			Vendeur v = new Vendeur();
			v.setCodePartenaire(r.getString("matricule"));
			v.setAdresse(r.getString("addr"));
			v.setDateDeNaissance(r.getDate("nais"));
			v.setEmplois(partenaireService.obtenirEmplois("VD"));
			v.setMatricule(r.getString("matricule"));
			v.setNom(r.getString("nom"));
			v.setPrenom(r.getString("prenom"));
			v.setRecoisDesCommission(r.getBoolean("com"));
			v.setTauxDeCommission(r.getDouble("tcom"));
			v.setSexe((r.getInt("sexe")==1)?Sexe.HOMME:Sexe.FEMME);
			Zone z = accept(localisationService, r.getString("zone"));
			if (z != null)
				v.setZoneDeVente(z);
			partenaireService.ajouterPartenaire(v);
		}
		// Caissiers

		sql = "select CodePersonnel matricule, NomPersonnel nom, PrenomPersonnel prenom, DateDenaissancePersonnel nais, Key_Timmo veh, AdressePersonnel addr,Key_Templois emplois, sexePersonnel sexe  from temploye where Key_Templois='CA'";
		r = m.execute(sql);
		while (r.next()) {
			Caissier v = new Caissier();
			v.setCodePartenaire(r.getString("matricule"));
			v.setAdresse(r.getString("addr"));
			v.setDateDeNaissance(r.getDate("nais"));
			v.setEmplois(partenaireService.obtenirEmplois(r.getString("emplois")));
			v.setMatricule(r.getString("matricule"));
			v.setNom(r.getString("nom"));
			v.setPrenom(r.getString("prenom"));
			v.setSexe((r.getInt("sexe")==1)?Sexe.HOMME:Sexe.FEMME);
			partenaireService.ajouterPartenaire(v);
		}
		
		
		//Magasinier
		
		  sql = "select CodePersonnel matricule, NomPersonnel nom, PrenomPersonnel prenom, DateDenaissancePersonnel nais, Key_Timmo veh, AdressePersonnel addr,Key_Templois emplois, sexePersonnel sexe  from temploye where Key_Templois in ('MG','AMG')";
	        r = m.execute(sql);
	        while (r.next()) {
	            Magasinier v = new Magasinier();
	            v.setCodePartenaire(r.getString("matricule"));
	            v.setAdresse(r.getString("addr"));
	            v.setDateDeNaissance(r.getDate("nais"));
	            v.setEmplois(partenaireService.obtenirEmplois(r.getString("emplois")));
	            v.setMatricule(r.getString("matricule"));
	            v.setNom(r.getString("nom"));
	            v.setPrenom(r.getString("prenom"));
	            v.setSexe((r.getInt("sexe")==1)?Sexe.HOMME:Sexe.FEMME);
	            partenaireService.ajouterPartenaire(v);
	        }


		// Autres employes

		sql = "select CodePersonnel matricule, NomPersonnel nom, PrenomPersonnel prenom, DateDenaissancePersonnel nais, Key_Timmo veh, AdressePersonnel addr,Key_Templois emplois,sexePersonnel sexe  from temploye where Key_Templois not in ('VD','CA','AMG','AD')";
		r = m.execute(sql);
		while (r.next()) {
			Employe v = new Employe();
			v.setCodePartenaire(r.getString("matricule"));
			v.setAdresse(r.getString("addr"));
			v.setDateDeNaissance(r.getDate("nais"));
			v.setEmplois(partenaireService.obtenirEmplois(r.getString("emplois")));
			v.setMatricule(r.getString("matricule"));
			v.setNom(r.getString("nom"));
			v.setPrenom(r.getString("prenom"));
			v.setSexe((r.getInt("sexe")==1)?Sexe.HOMME:Sexe.FEMME);
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
			Zone o = accept(localisationService, r.getString("zone"));
			if (o != null)
				c.setZone(o);
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
			Zone o = accept(localisationService, r.getString("zone"));
			if (o != null)
				c.setZone(o);
			partenaireService.ajouterPartenaire(c);
		}

		// Vehicules
		sql = "select CodeImmobilisation code, IntituleImmobilisation description from timmobilisation where CodeImmobilisation like '%VEH%'";
		r = m.execute(sql);
		while (r.next()) {
			Vehicule v = new Vehicule();
			v.setCodeImmo(r.getString("code"));
			v.setImmatriculation(r.getString("code"));
			v.setDescriptionImmo(r.getString("description"));
			partenaireService.ajouterImmobilisation(v);
		}
		
		// Vitrines
		sql = "select CodeImmobilisation code, IntituleImmobilisation description from timmobilisation where CodeImmobilisation not like '%VEH%'";
		r = m.execute(sql);
		while (r.next()) {
			Refrigerateur v = new Refrigerateur();
			v.setCodeImmo(r.getString("code"));
			v.setDescriptionImmo(r.getString("description"));
			partenaireService.ajouterImmobilisation(v);
		}
		
		//Partenaire immo.
		sql="select p.key_tpartenaire partenaire, Key_Timmo immo, DateObtention date from tpartenairettypepartenaireimmobilisation i join tpartenairettypepartenaire p on i.Key_TpartenaireTypePartenaire=p.Key_TpartenaireTypePartenaire";
		r = m.execute(sql);
        while (r.next()) {
            PartenaireImmobilisation p = new PartenaireImmobilisation();
            p.setActif(true);
            p.setDateAllocation(r.getDate("date"));
            p.setImmobilisation(partenaireService.obtenirImmobilisation(Refrigerateur.class, r.getString("immo")));
            p.setPartenaire(partenaireService.obtenirPartenaire(Client.class, r.getString("partenaire")));
            partenaireService.ajouterPartenaireImmobilisation(p);          
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

		sql = "select CodeSecteur code, IntituleSecteur description, Key_Tregion region from tsecteur where  CodeSecteur<>'D1' and Key_tregion is not null and Key_Tregion<>'R1'";
		r = m.execute(sql);
		while (r.next()) {
			Secteur s = new Secteur();
			s.setCode(r.getString("code"));
			s.setDescriptionLocal(r.getString("description"));
			s.setRegion(localService.obtenirLocalisation(Region.class, r.getString("region")));
			localService.ajouterLocalisation(s);
		}

		sql = "select CodeCentre code, IntituleZone description, CodeSecteur secteur from tzone where CodeSecteur not in ('COM01','D1','CBASSA')";
		r = m.execute(sql);
		while (r.next()) {
			Zone z = new Zone();
			z.setCode(r.getString("code"));
			z.setDescriptionLocal(r.getString("description"));
			z.setSecteur(localService.obtenirLocalisation(Secteur.class, r.getString("secteur")));
			localService.ajouterLocalisation(z);
		}
	}

	public static void entrepot(IInventaireService inventaire, ILocalisationService local, AbstractConnection m)
			throws SQLException {
		String sql = "";
		ResultSet r = null;

		// Entrepots
		sql = "select CodeDepot code , IntituleDepot description, CodeCentre centre from tdepots";
		r = m.execute(sql);
		while (r.next()) {
			Entrepot e = new Entrepot();
			e.setCentre(local.obtenirLocalisation(Centre.class, r.getString("centre")));
			e.setCodeEntrepot(r.getString("code"));
			e.setDescriptionEntrepot(r.getString("description"));
			inventaire.ajouterEntrepot(e);
		}

		// Magasins mobiles

		sql = "select CodeMagasin code, IntituleMagasin description, Key_Tdepot depot from tmagasin where MagasinAmbulant=1";
		r = m.execute(sql);
		while (r.next()) {
			MagasinMobile ma = new MagasinMobile();
			ma.setCodeMagasin(r.getString("code"));
			ma.setDescriptionMagasin(r.getString("description"));
			ma.setEntrepot(inventaire.obtenirEntrepot(r.getString("depot")));
			inventaire.ajouterMagasin(ma);
		}

		// Magasins fixes

		sql = "select CodeMagasin code, IntituleMagasin description, Key_Tdepot depot from tmagasin where MagasinAmbulant=0";
		r = m.execute(sql);
		while (r.next()) {
			MagasinFixe ma = new MagasinFixe();
			ma.setCodeMagasin(r.getString("code"));
			ma.setDescriptionMagasin(r.getString("description"));
			ma.setEntrepot(inventaire.obtenirEntrepot(r.getString("depot")));
			inventaire.ajouterMagasin(ma);
		}
	}

	private static List<String> blackList = Arrays.asList("NDERE", "SECT01", TO_REMOVE, "", "NDG01CDOG", "SECT03CDOG");

	private static Zone accept(ILocalisationService localisationService, String z) {
		Localisation l = null;
		if ((!blackList.contains(z)) && z != null && (!"".equals(z))) {
			l = localisationService.obtenirLocalisation(Zone.class, z);
		}

		if (l instanceof Zone)
			return (Zone) l;
		return null;
	}
}
