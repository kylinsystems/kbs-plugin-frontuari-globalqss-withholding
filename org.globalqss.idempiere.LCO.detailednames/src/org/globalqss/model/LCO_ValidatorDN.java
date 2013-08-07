/******************************************************************************
 * Product: iDempiere ERP & CRM Smart Business Solution                       *
 * Copyright (C) 1999-2006 ComPiere, Inc. All Rights Reserved.                *
 * This program is free software; you can redistribute it and/or modify it    *
 * under the terms version 2 of the GNU General Public License as published   *
 * by the Free Software Foundation. This program is distributed in the hope   *
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the implied *
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.           *
 * See the GNU General Public License for more details.                       *
 * You should have received a copy of the GNU General Public License along    *
 * with this program; if not, write to the Free Software Foundation, Inc.,    *
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.                     *
 * For the text or an alternative of this public license, you may reach us    *
 * ComPiere, Inc., 2620 Augustine Dr. #245, Santa Clara, CA 95054, USA        *
 * or via info@compiere.org or http://www.compiere.org/license.html           *
 *****************************************************************************/
package org.globalqss.model;

import org.compiere.model.MBPartner;
import org.compiere.model.MClient;
import org.compiere.model.ModelValidationEngine;
import org.compiere.model.ModelValidator;
import org.compiere.model.PO;
import org.compiere.util.CLogger;
import org.compiere.util.Msg;
import org.globalqss.util.LCO_Utils;

/**
 *	Validator or Localization Colombia (Detailed Names)
 *	
 *  @author Carlos Ruiz - globalqss - Quality Systems & Solutions - http://globalqss.com 
 *	@version $Id: LCO_Validator.java,v 1.4 2007/05/13 06:53:26 cruiz Exp $
 */
public class LCO_ValidatorDN implements ModelValidator
{
	/**
	 *	Constructor.
	 *	The class is instantiated when logging in and client is selected/known
	 */
	public LCO_ValidatorDN ()
	{
		super ();
	}	//	MyValidator
	
	/**	Logger			*/
	private static CLogger log = CLogger.getCLogger(LCO_ValidatorDN.class);
	/** Client			*/
	private int		m_AD_Client_ID = -1;
	
	/**
	 *	Initialize Validation
	 *	@param engine validation engine 
	 *	@param client client
	 */
	public void initialize (ModelValidationEngine engine, MClient client)
	{
		//client = null for global validator
		if (client != null) {	
			m_AD_Client_ID = client.getAD_Client_ID();
			log.info(client.toString());
		}
		else  {
			log.info("Initializing global validator: "+this.toString());
		}

		//	Tables to be monitored
		engine.addModelChange(MBPartner.Table_Name, this);
		engine.addModelChange(X_LCO_TaxIdType.Table_Name, this);

		//	Documents to be monitored

	}	//	initialize

    /**
     *	Model Change of a monitored Table.
     *	Called after PO.beforeSave/PO.beforeDelete
     *	when you called addModelChange for the table
     *	@param po persistent object
     *	@param type TYPE_
     *	@return error message or null
     *	@exception Exception if the recipient wishes the change to be not accept.
     */
	public String modelChange (PO po, int type) throws Exception
	{
		log.info(po.get_TableName() + " Type: "+type);
		String msg;

		// Check Digit based on TaxID
		if (po.get_TableName().equals(MBPartner.Table_Name) && ( type == TYPE_BEFORE_NEW || type == TYPE_BEFORE_CHANGE))
		{
			MBPartner bpartner = (MBPartner)po;
			msg = mcheckTaxIdDigit(bpartner);
			if (msg != null)
				return msg;

			msg = mfillName(bpartner);
			if (msg != null)
				return msg;
		}
		
		if (po.get_TableName().equals(X_LCO_TaxIdType.Table_Name) && ( type == TYPE_BEFORE_NEW || type == TYPE_BEFORE_CHANGE))
		{
			X_LCO_TaxIdType taxidtype = (X_LCO_TaxIdType) po;
			if ((!taxidtype.isUseTaxIdDigit()) && taxidtype.isDigitChecked())
				taxidtype.setIsDigitChecked(false);
		}
		
		return null;
	}	//	modelChange
	
	/**
	 *	Validate Document.
	 *	Called as first step of DocAction.prepareIt 
     *	when you called addDocValidate for the table.
     *	Note that totals, etc. may not be correct.
	 *	@param po persistent object
	 *	@param timing see TIMING_ constants
     *	@return error message or null
	 */
	public String docValidate (PO po, int timing)
	{
		log.info(po.get_TableName() + " Timing: "+timing);

		return null;
	}	//	docValidate

	/**
	 *	User Login.
	 *	Called when preferences are set
	 *	@param AD_Org_ID org
	 *	@param AD_Role_ID role
	 *	@param AD_User_ID user
	 *	@return error message or null
	 */
	public String login (int AD_Org_ID, int AD_Role_ID, int AD_User_ID)
	{
		log.info("AD_User_ID=" + AD_User_ID);
		return null;
	}	//	login

	
	/**
	 *	Get Client to be monitored
	 *	@return AD_Client_ID client
	 */
	public int getAD_Client_ID()
	{
		return m_AD_Client_ID;
	}	//	getAD_Client_ID

	
	/**
	 * 	String Representation
	 *	@return info
	 */
	public String toString ()
	{
		StringBuffer sb = new StringBuffer ("LCO_Validator");
		return sb.toString ();
	}	//	toString
	
	/**
	 *	Check Digit based on TaxID.
	 */
	private String mcheckTaxIdDigit (MBPartner bpartner)
	{
		Integer taxidtype_I = (Integer) bpartner.get_Value("LCO_TaxIdType_ID");
		
		if (taxidtype_I == null) {
			// Returning error here has problems with Initial Client Setup and other processes
			// that creates BPs
			// Mandatory must be delegated to UI (in AD_Field.ismandatory)
			// return Msg.getMsg(bpartner.getCtx(), "LCO_TaxIDTypeRequired");
			return null;
		}
		
		X_LCO_TaxIdType taxidtype = new X_LCO_TaxIdType(bpartner.getCtx(), taxidtype_I.intValue(), bpartner.get_TrxName());
		
		bpartner.set_ValueOfColumn("IsDetailedNames", taxidtype.isDetailedNames());
		bpartner.set_ValueOfColumn("IsUseTaxIdDigit", taxidtype.isUseTaxIdDigit());
		
		if (!taxidtype.isUseTaxIdDigit()) {
			bpartner.set_ValueOfColumn("TaxIdDigit", null);
			return null;
		}
			
		// Is Juridical
		String taxid = bpartner.getTaxID();
		if (taxid == null || taxid.trim().length() == 0)
			return Msg.getMsg(bpartner.getCtx(), "LCO_NoTaxID");

		int correctDigit = LCO_Utils.calculateDigitDian(taxid);
		if (correctDigit == -1) // Error on the Tax ID - possibly invalid characters
			return Msg.getMsg(bpartner.getCtx(), "LCO_NotValidID");

		String taxIDDigit = (String) bpartner.get_Value("TaxIdDigit");
		if (taxidtype.isDigitChecked()) {
			if (taxIDDigit == null || taxIDDigit.trim().length() == 0)
				return Msg.getMsg(bpartner.getCtx(), "LCO_NoDigit"); // No Tax ID Digit
			int taxIDDigit_int;
			try {
				taxIDDigit_int = Integer.parseInt(taxIDDigit);
			} catch (NumberFormatException e) {
				return Msg.getMsg(bpartner.getCtx(), "LCO_NotANumber");  // Error on the check digit
			}
			if (correctDigit != taxIDDigit_int)
				return Msg.getMsg(bpartner.getCtx(), "LCO_VerifyCheckDigit");
		} else {
			bpartner.set_ValueOfColumn("TaxIdDigit", String.valueOf(correctDigit));
		}
		
		log.info(bpartner.toString());
		return null;
	}	//	mcheckTaxIdDigit

	/**
	 * 	Fill Name based on First and Last Names
	 *	@param bpartner bpartner
	 *	@return error message or null
	 */
	public String mfillName (MBPartner bpartner)
	{
		log.info("");
		boolean isDetailedNames = false;
		Boolean boolIsDetailedNames = (Boolean)bpartner.get_Value("IsDetailedNames");
		if (boolIsDetailedNames != null)
			isDetailedNames = boolIsDetailedNames.booleanValue();
		
		if (! isDetailedNames) {
			bpartner.set_ValueOfColumn("FirstName1", null);
			bpartner.set_ValueOfColumn("FirstName2", null);
			bpartner.set_ValueOfColumn("LastName1", null);
			bpartner.set_ValueOfColumn("LastName2", null);
			return null;
		}

		String fn1 = bpartner.get_ValueAsString("FirstName1");
		String fn2 = bpartner.get_ValueAsString("FirstName2");
		String ln1 = bpartner.get_ValueAsString("LastName1");
		String ln2 = bpartner.get_ValueAsString("LastName2");
		
		if (fn1 == null || fn1.length() == 0)
			 return Msg.getMsg(bpartner.getCtx(), "LCO_FirstName1Required");

		 if (ln1 == null || ln1.length() == 0)
			return Msg.getMsg(bpartner.getCtx(), "LCO_LastName1Required");

		String fullName = LCO_Utils.getFullName(fn1, fn2, ln1, ln2, bpartner.getAD_Client_ID());
		bpartner.setName(fullName);
		return null;
	}	//	mfillName
	
}	//	LCO_ValidatorDN