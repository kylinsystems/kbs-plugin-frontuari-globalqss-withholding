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

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;

import org.adempiere.base.event.AbstractEventHandler;
import org.adempiere.base.event.FactsEventData;
import org.adempiere.base.event.IEventTopics;
import org.compiere.acct.Doc;
import org.compiere.acct.DocLine_Allocation;
import org.compiere.acct.DocTax;
import org.compiere.acct.Fact;
import org.compiere.acct.FactLine;
import org.compiere.model.MAcctSchema;
import org.compiere.model.MAllocationHdr;
import org.compiere.model.MAllocationLine;
import org.compiere.model.MDocType;
import org.compiere.model.MInvoice;
import org.compiere.model.MInvoiceLine;
import org.compiere.model.MInvoicePaySchedule;
import org.compiere.model.MInvoiceTax;
import org.compiere.model.MPayment;
import org.compiere.model.MPaymentAllocate;
import org.compiere.model.PO;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Msg;
import org.osgi.service.event.Event;

/**
 *	Validator or Localization Colombia (Withholdings)
 *	
 *  @author Carlos Ruiz - globalqss - Quality Systems & Solutions - http://globalqss.com 
 *	@version $Id: LCO_Validator.java,v 1.4 2007/05/13 06:53:26 cruiz Exp $
 */
public class LCO_ValidatorWH extends AbstractEventHandler
{
	/**	Logger			*/
	private static CLogger log = CLogger.getCLogger(LCO_ValidatorWH.class);
	
	/**
	 *	Initialize Validation
	 */
	@Override
	protected void initialize() {
		//	Tables to be monitored
		
		
		registerTableEvent(IEventTopics.PO_BEFORE_CHANGE, MInvoice.Table_Name);
		registerTableEvent(IEventTopics.PO_BEFORE_NEW, MInvoiceLine.Table_Name);
		registerTableEvent(IEventTopics.PO_BEFORE_CHANGE, MInvoiceLine.Table_Name);
		registerTableEvent(IEventTopics.PO_BEFORE_DELETE, MInvoiceLine.Table_Name);
		registerTableEvent(IEventTopics.PO_BEFORE_NEW, X_LCO_WithholdingCalc.Table_Name);
		registerTableEvent(IEventTopics.PO_BEFORE_CHANGE, X_LCO_WithholdingCalc.Table_Name);

		//	Documents to be monitored
		registerTableEvent(IEventTopics.DOC_BEFORE_PREPARE, MInvoice.Table_Name);
		registerTableEvent(IEventTopics.DOC_BEFORE_COMPLETE, MInvoice.Table_Name);
		registerTableEvent(IEventTopics.DOC_AFTER_COMPLETE, MInvoice.Table_Name);
		//registerTableEvent(IEventTopics.DOC_BEFORE_COMPLETE, MPayment.Table_Name);
		registerTableEvent(IEventTopics.DOC_BEFORE_COMPLETE, MAllocationHdr.Table_Name);
		registerTableEvent(IEventTopics.DOC_AFTER_COMPLETE, MAllocationHdr.Table_Name);
		//registerTableEvent(IEventTopics.DOC_BEFORE_POST, MAllocationHdr.Table_Name);
		//registerTableEvent(IEventTopics.DOC_AFTER_POST, MAllocationHdr.Table_Name);
		registerTableEvent(IEventTopics.ACCT_FACTS_VALIDATE, MAllocationHdr.Table_Name);
		registerTableEvent(IEventTopics.DOC_AFTER_VOID, MAllocationHdr.Table_Name);
		registerTableEvent(IEventTopics.DOC_AFTER_REVERSECORRECT, MAllocationHdr.Table_Name);
		registerTableEvent(IEventTopics.DOC_AFTER_REVERSEACCRUAL, MAllocationHdr.Table_Name);
	}	//	initialize

    /**
     *	Model Change of a monitored Table or Document
     *  @param event
     *	@exception Exception if the recipient wishes the change to be not accept.
     */
	@Override
	protected void doHandleEvent(Event event) {
		PO po = null;
		String type = event.getTopic();
		
		if (type.equals(IEventTopics.ACCT_FACTS_VALIDATE)) {
			FactsEventData fed = getEventData(event);
			po = fed.getPo();
		} else {
			po = getPO(event);
		}
		log.info(po.get_TableName() + " Type: "+type);
		String msg;
		
		// Model Events
		if (po.get_TableName().equals(MInvoice.Table_Name) && type.equals(IEventTopics.PO_BEFORE_CHANGE)) {
			msg = clearInvoiceWithholdingAmtFromInvoice((MInvoice) po);
			if (msg != null)
				throw new RuntimeException(msg);
		}

		// when invoiceline is changed clear the withholding amount on invoice
		// in order to force a regeneration
		if (po.get_TableName().equals(MInvoiceLine.Table_Name) &&
				(type.equals(IEventTopics.PO_BEFORE_NEW) ||
				 type.equals(IEventTopics.PO_BEFORE_CHANGE) ||
				 type.equals(IEventTopics.PO_BEFORE_DELETE)
				)
			)
		{
			msg = clearInvoiceWithholdingAmtFromInvoiceLine((MInvoiceLine) po, type);
			if (msg != null)
				throw new RuntimeException(msg);
		}

		if (po.get_TableName().equals(X_LCO_WithholdingCalc.Table_Name)
				&& (type.equals(IEventTopics.PO_BEFORE_CHANGE) || type.equals(IEventTopics.PO_BEFORE_NEW))) {
			X_LCO_WithholdingCalc lwc = (X_LCO_WithholdingCalc) po;
			if (lwc.isCalcOnInvoice() && lwc.isCalcOnPayment())
				lwc.setIsCalcOnPayment(false);
		}
		
		// Document Events
		// before preparing a reversal invoice add the invoice withholding taxes
		if (po.get_TableName().equals(MInvoice.Table_Name)
				&& type.equals(IEventTopics.DOC_BEFORE_PREPARE)) {
			MInvoice inv = (MInvoice) po;
			if(!inv.isSOTrx()){//added by Adonis Castellanos 24/09/2020 
				if (inv.isReversal()) {
					int invid = inv.getReversal_ID();
					
					if (invid > 0) {
						MInvoice invreverted = new MInvoice(inv.getCtx(), invid, inv.get_TrxName());
						String sql = 
							  "SELECT LCO_InvoiceWithholding_ID "
							 + " FROM LCO_InvoiceWithholding "
							+ " WHERE C_Invoice_ID = ? "
							+ " ORDER BY LCO_InvoiceWithholding_ID";
						PreparedStatement pstmt = null;
						ResultSet rs = null;
						try
						{
							pstmt = DB.prepareStatement(sql, inv.get_TrxName());
							pstmt.setInt(1, invreverted.getC_Invoice_ID());
							rs = pstmt.executeQuery();
							while (rs.next()) {
								MLCOInvoiceWithholding iwh = new MLCOInvoiceWithholding(inv.getCtx(), rs.getInt(1), inv.get_TrxName());
								MLCOInvoiceWithholding newiwh = new MLCOInvoiceWithholding(inv.getCtx(), 0, inv.get_TrxName());
								newiwh.setAD_Org_ID(iwh.getAD_Org_ID());
								newiwh.setC_Invoice_ID(inv.getC_Invoice_ID());
								newiwh.setLCO_WithholdingType_ID(iwh.getLCO_WithholdingType_ID());
								newiwh.setPercent(iwh.getPercent());
								newiwh.setTaxAmt(iwh.getTaxAmt().negate());
								newiwh.setTaxBaseAmt(iwh.getTaxBaseAmt().negate());
								newiwh.setC_Tax_ID(iwh.getC_Tax_ID());
								newiwh.setIsCalcOnPayment(iwh.isCalcOnPayment());
								newiwh.setIsActive(iwh.isActive());	// Reviewme
								if (!newiwh.save())
									throw new RuntimeException("Error saving LCO_InvoiceWithholding docValidate");
							}
						} catch (Exception e) {
							log.log(Level.SEVERE, sql, e);
							throw new RuntimeException("Error creating LCO_InvoiceWithholding for reversal invoice");
						} finally {
							DB.close(rs, pstmt);
							rs = null; pstmt = null;
						}
					} else {
						throw new RuntimeException("Can't get the number of the invoice reversed");
					}
				}
			}	
		}

		// before preparing invoice validate if withholdings has been generated
		if (po.get_TableName().equals(MInvoice.Table_Name)
				&& type.equals(IEventTopics.DOC_BEFORE_PREPARE)) {
			MInvoice inv = (MInvoice) po;
			/* @TODO: Change this to IsReversal & Reversal_ID on 3.5 */
			if(!inv.isSOTrx()){//added by Adonis Castellanos 24/09/2020 
				if (inv.getDescription() != null 
						&& inv.getDescription().contains("{->")
						&& inv.getDescription().endsWith(")")) {
					// don't validate this for autogenerated reversal invoices
				} else {
					if (inv.get_Value("WithholdingAmt") == null) {
						MDocType dt = new MDocType(inv.getCtx(), inv.getC_DocTypeTarget_ID(), inv.get_TrxName());
						String genwh = dt.get_ValueAsString("GenerateWithholding");
						if (genwh != null) {
	
	//						if (genwh.equals("Y")) {
	//							// document type configured to compel generation of withholdings
	//							throw new RuntimeException(Msg.getMsg(inv.getCtx(), "LCO_WithholdingNotGenerated"));
	//						}
							
							if (genwh.equals("A")) {
								// document type configured to generate withholdings automatically
								LCO_MInvoice lcoinv = new LCO_MInvoice(inv.getCtx(), inv.getC_Invoice_ID(), inv.get_TrxName());
								try {
									lcoinv.recalcWithholdings(null);
								} catch (SQLException e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			}	
		}

		// after preparing invoice move invoice withholdings to taxes and recalc grandtotal of invoice
		if (po.get_TableName().equals(MInvoice.Table_Name) && type.equals(IEventTopics.DOC_BEFORE_COMPLETE)) {
			MInvoice inv = (MInvoice) po;
			if(!inv.isSOTrx()){//added by Adonis Castellanos 24/09/2020 
				msg = translateWithholdingToTaxes(inv);
				if (msg != null)
					throw new RuntimeException(msg);
			}	
		}

		// after completing the invoice fix the dates on withholdings and mark the invoice withholdings as processed
		if (po.get_TableName().equals(MInvoice.Table_Name) && type.equals(IEventTopics.DOC_AFTER_COMPLETE)) {
			MInvoice inv = (MInvoice) po;
			if(!inv.isSOTrx()){//added by Adonis Castellanos 24/09/2020 
				msg = completeInvoiceWithholding((MInvoice) po);
				if (msg != null)
					throw new RuntimeException(msg);
			}	
		}

		// before completing the payment - validate that writeoff amount must be greater than sum of payment withholdings  
//		if (po.get_TableName().equals(MPayment.Table_Name) && type.equals(IEventTopics.DOC_BEFORE_COMPLETE)) {
//			msg = validateWriteOffVsPaymentWithholdings((MPayment) po);
//			if (msg != null)
//				throw new RuntimeException(msg);
//		}

		// update allocation org when comes from withholding and the orgs aren't the same
		if (po.get_TableName().equals(MAllocationHdr.Table_Name) && type.equals(IEventTopics.DOC_BEFORE_COMPLETE)) {
			MAllocationHdr all = (MAllocationHdr)po;
			 String sql = "SELECT CASE WHEN allol.ad_org_id <> vw.ad_org_id THEN vw.ad_org_id ELSE 0 END FROM c_allocationline allol"
			 		+ " JOIN c_payment p ON allol.c_payment_id = p.c_payment_id"
			 		+ " JOIN c_paymentallocate pal ON p.c_payment_id = pal.c_payment_id"
			 		+ " JOIN LCO_InvoiceWithholding iw ON pal.LCO_InvoiceWithholding_ID = iw.lco_invoicewithholding_id"
			 		+ " JOIN lve_voucherwithholding vw ON iw.lve_voucherwithholding_id = vw.lve_voucherwithholding_id"
			 		+ " WHERE allol.c_allocationhdr_id ="+all.getC_AllocationHdr_ID();
			 int AD_Org_ID = DB.getSQLValue(po.get_TrxName(), sql);
			 if(AD_Org_ID>0) {
				 all.setAD_Org_ID(AD_Org_ID);
				 all.saveEx(po.get_TrxName());
			 }
		}

		// after completing the allocation - complete the payment withholdings  
		if (po.get_TableName().equals(MAllocationHdr.Table_Name) && type.equals(IEventTopics.DOC_AFTER_COMPLETE)) {
			msg = completePaymentWithholdings((MAllocationHdr) po);
			if (msg != null)
				throw new RuntimeException(msg);
		}

		// before posting the allocation - post the payment withholdings vs writeoff amount  
		/*if (po.get_TableName().equals(MAllocationHdr.Table_Name) && type.equals(IEventTopics.DOC_BEFORE_POST)) {
			msg = accountingForInvoiceWithholdingOnPayment((MAllocationHdr) po);
			if (msg != null)
				throw new RuntimeException(msg);
		}*/
		
		// before posting the allocation - post the payment withholdings vs writeoff amount
		if (po instanceof MAllocationHdr && type.equals(IEventTopics.ACCT_FACTS_VALIDATE)) {
			msg = accountingForInvoiceWithholdingOnPayment((MAllocationHdr) po, event);
			if (msg != null)
				throw new RuntimeException(msg);
		}
		

		// after completing the allocation - complete the payment withholdings  
		if (po.get_TableName().equals(MAllocationHdr.Table_Name)
				&& (type.equals(IEventTopics.DOC_AFTER_VOID) || 
					type.equals(IEventTopics.DOC_AFTER_REVERSECORRECT) || 
					type.equals(IEventTopics.DOC_AFTER_REVERSEACCRUAL))) {
			msg = reversePaymentWithholdings((MAllocationHdr) po);
			if (msg != null)
				throw new RuntimeException(msg);
		}
		
	}	//	doHandleEvent
	
	private String clearInvoiceWithholdingAmtFromInvoice(MInvoice inv) {
		// Clear invoice withholding amount
		
		if (inv.is_ValueChanged("AD_Org_ID")
				|| inv.is_ValueChanged(MInvoice.COLUMNNAME_C_BPartner_ID)
				|| inv.is_ValueChanged(MInvoice.COLUMNNAME_C_DocTypeTarget_ID)) {
			
			boolean thereAreCalc;
			try {
				thereAreCalc = thereAreCalc(inv);
			} catch (SQLException e) {
				log.log(Level.SEVERE, "Error looking for calc on invoice rules", e);
				return "Error looking for calc on invoice rules";
			}
			
			BigDecimal curWithholdingAmt = (BigDecimal) inv.get_Value("WithholdingAmt");
			if (thereAreCalc) {
				if (curWithholdingAmt != null) {
					inv.set_CustomColumn("WithholdingAmt", null);
				}
			} else {
				if (curWithholdingAmt == null) {
					inv.set_CustomColumn("WithholdingAmt", Env.ZERO);
				}
			}

		}
		
		return null;
	}

	private String clearInvoiceWithholdingAmtFromInvoiceLine(MInvoiceLine invline, String type) {
		
		if (   type.equals(IEventTopics.PO_BEFORE_NEW)
			|| type.equals(IEventTopics.PO_BEFORE_DELETE)
			|| (   type.equals(IEventTopics.PO_BEFORE_CHANGE) 
				&& (   invline.is_ValueChanged("LineNetAmt")
					|| invline.is_ValueChanged("M_Product_ID")
					|| invline.is_ValueChanged("C_Charge_ID")
					|| invline.is_ValueChanged("IsActive") 
					|| invline.is_ValueChanged("C_Tax_ID")
					)
				)
			) 
		{
			// Clear invoice withholding amount
			MInvoice inv = invline.getParent();

			boolean thereAreCalc;
			try {
				thereAreCalc = thereAreCalc(inv);
			} catch (SQLException e) {
				log.log(Level.SEVERE, "Error looking for calc on invoice rules", e);
				return "Error looking for calc on invoice rules";
			}

			BigDecimal curWithholdingAmt = (BigDecimal) inv.get_Value("WithholdingAmt");
			if (thereAreCalc) {
				if (curWithholdingAmt != null) {
					if (!LCO_MInvoice.setWithholdingAmtWithoutLogging(inv, null))
						return "Error saving C_Invoice clearInvoiceWithholdingAmtFromInvoiceLine";
				}
			} else {
				if (curWithholdingAmt == null) {
					if (!LCO_MInvoice.setWithholdingAmtWithoutLogging(inv, Env.ZERO))
						return "Error saving C_Invoice clearInvoiceWithholdingAmtFromInvoiceLine";
				}
			}
		}
		
		return null;
	}

	private boolean thereAreCalc(MInvoice inv) throws SQLException {
		boolean thereAreCalc = false;
		String sqlwccoi = 
			"SELECT 1 "
			+ "  FROM LCO_WithholdingType wt, LCO_WithholdingCalc wc "
			+ " WHERE wt.LCO_WithholdingType_ID = wc.LCO_WithholdingType_ID";
		PreparedStatement pstmtwccoi = DB.prepareStatement(sqlwccoi, inv.get_TrxName());
		ResultSet rswccoi = null;
		try {
			rswccoi = pstmtwccoi.executeQuery();
			if (rswccoi.next())
				thereAreCalc = true;
		} catch (SQLException e) {
			throw e;
		} finally {
			DB.close(rswccoi, pstmtwccoi);
			rswccoi = null; pstmtwccoi = null;
		}
		return thereAreCalc;
	}

	private String validateWriteOffVsPaymentWithholdings(MPayment pay) {
		if (pay.getC_Invoice_ID() > 0) {
			// validate vs invoice of payment
			BigDecimal wo = pay.getWriteOffAmt();
			BigDecimal sumwhamt = Env.ZERO;
			
			sumwhamt = DB.getSQLValueBD(
					pay.get_TrxName(),
					"SELECT COALESCE (SUM (TaxAmt), 0) " +
					"FROM LCO_InvoiceWithholding " +
					"WHERE C_Invoice_ID = ? AND " +
					"IsActive = 'Y' AND " +
					"IsCalcOnPayment = 'Y' AND " +
					"Processed = 'N' AND " +
					"C_AllocationLine_ID IS NULL",
					pay.getC_Invoice_ID());
			if (sumwhamt == null)
				sumwhamt = Env.ZERO;
			if (wo.compareTo(sumwhamt) < 0 && sumwhamt.compareTo(Env.ZERO) != 0)
				return Msg.getMsg(pay.getCtx(), "LCO_WriteOffLowerThanWithholdings");
		} else {
			// validate every C_PaymentAllocate
			String sql = 
				"SELECT C_PaymentAllocate_ID " +
				"FROM C_PaymentAllocate " +
				"WHERE C_Payment_ID = ?";
			PreparedStatement pstmt = DB.prepareStatement(sql, pay.get_TrxName());
			ResultSet rs = null;
			try {
				pstmt.setInt(1, pay.getC_Payment_ID());
				rs = pstmt.executeQuery();
				while (rs.next()) {
					int palid = rs.getInt(1);
					MPaymentAllocate pal = new MPaymentAllocate(pay.getCtx(), palid, pay.get_TrxName());
					BigDecimal wo = pal.getWriteOffAmt();
					BigDecimal sumwhamt = Env.ZERO;
					sumwhamt = DB.getSQLValueBD(
							pay.get_TrxName(),
							"SELECT COALESCE (SUM (TaxAmt), 0) " +
							"FROM LCO_InvoiceWithholding " +
							"WHERE C_Invoice_ID = ? AND " +
							"IsActive = 'Y' AND " +
							"IsCalcOnPayment = 'Y' AND " +
							"Processed = 'N' AND " +
							"C_AllocationLine_ID IS NULL",
							pal.getC_Invoice_ID());
					if (sumwhamt == null)
						sumwhamt = Env.ZERO;
					if (wo.compareTo(sumwhamt) < 0 && sumwhamt.compareTo(Env.ZERO) != 0)
						return Msg.getMsg(pay.getCtx(), "LCO_WriteOffLowerThanWithholdings");
				}
			} catch (SQLException e) {
				e.printStackTrace();
				return e.getLocalizedMessage();
			} finally {
				DB.close(rs, pstmt);
				rs = null; pstmt = null;
			}
		}

		return null;
	}

	private String completePaymentWithholdings(MAllocationHdr ah) {
		MAllocationLine[] als = ah.getLines(true);
		for (int i = 0; i < als.length; i++) {
			MAllocationLine al = als[i];
			if (al.getC_Invoice_ID() > 0) {
				MPaymentAllocate[] pa = MPaymentAllocate.get((MPayment)al.getC_Payment());	
				for(MPaymentAllocate line : pa)
				{
					/*MLCOInvoiceWithholding iwh = new MLCOInvoiceWithholding(
							ah.getCtx(), line.get_ValueAsInt("LCO_InvoiceWithholding_ID"), ah.get_TrxName());
					iwh.setC_AllocationLine_ID(al.getC_AllocationLine_ID());
					iwh.setDateAcct(ah.getDateAcct());
					iwh.setDateTrx(ah.getDateTrx());
					iwh.setProcessed(true);
					if (!iwh.save())
						return "Error saving LCO_InvoiceWithholding completePaymentWithholdings";
						*/
					/*String sql = "UPDATE LCO_InvoiceWithholding SET C_AllocationLine_ID=?,DateAcct=?,DateTrx=?,Processed='Y' WHERE C_Payment_ID = ? ";
					DB.executeUpdate(sql,new Object[] {al.getC_AllocationLine_ID(),ah.getDateAcct(),ah.getDateTrx(),al.getC_Payment_ID()},true,line.get_TrxName());*/
					if(al.getC_Invoice_ID()!=line.getC_Invoice_ID())
						continue;
					String sql = "UPDATE LCO_InvoiceWithholding SET C_AllocationLine_ID="+al.getC_AllocationLine_ID()+",DateAcct='"+ah.getDateAcct()
					+ "',DateTrx='"+ah.getDateTrx()+"',Processed='Y' WHERE LCO_InvoiceWithholding_ID="+line.get_ValueAsInt("LCO_InvoiceWithholding_ID");
					//System.out.println(sql);
					DB.executeUpdate(sql,true,line.get_TrxName());
					
				}
			}
		}
		return null;
	}

	private String reversePaymentWithholdings(MAllocationHdr ah) {
		MAllocationLine[] als = ah.getLines(true);
		for (int i = 0; i < als.length; i++) {
			MAllocationLine al = als[i];
			if (al.getC_Invoice_ID() > 0) {
				String sql = 
					"SELECT LCO_InvoiceWithholding_ID " +
					"FROM LCO_InvoiceWithholding " +
					"WHERE C_Invoice_ID = ? AND " +
					"IsActive = 'Y' AND " +
					"IsCalcOnPayment = 'Y' AND " +
					"Processed = 'Y' AND " +
					"C_AllocationLine_ID = ?";
				PreparedStatement pstmt = DB.prepareStatement(sql, ah.get_TrxName());
				ResultSet rs = null;
				try {
					pstmt.setInt(1, al.getC_Invoice_ID());
					pstmt.setInt(2, al.getC_AllocationLine_ID());
					rs = pstmt.executeQuery();
					while (rs.next()) {
						int iwhid = rs.getInt(1);
						MLCOInvoiceWithholding iwh = new MLCOInvoiceWithholding(
								ah.getCtx(), iwhid, ah.get_TrxName());
						iwh.setC_AllocationLine_ID(0);
						iwh.setProcessed(false);
						if (!iwh.save())
							return "Error saving LCO_InvoiceWithholding reversePaymentWithholdings";
					}
				} catch (SQLException e) {
					e.printStackTrace();
					return e.getLocalizedMessage();
				} finally {
					DB.close(rs, pstmt);
					rs = null; pstmt = null;
				}
			}
		}
		return null;
	}

	private String accountingForInvoiceWithholdingOnPayment(MAllocationHdr ah, Event event) {
		// Accounting like Doc_Allocation
		// (Write off) vs (invoice withholding where iscalconpayment=Y)
		// 20070807 - globalqss - instead of adding a new WriteOff post, find the
		//  current WriteOff and subtract from the posting
		
		Doc doc = ah.getDoc();
		FactsEventData fed = getEventData(event);
		List<Fact> facts = fed.getFacts();
		
		
		// one fact per acctschema
		for (int i = 0; i < facts.size(); i++)
		{
			Fact fact = facts.get(i);
			MAcctSchema as = fact.getAcctSchema();
			
			MAllocationLine[] alloc_lines = ah.getLines(false);
			for (int j = 0; j < alloc_lines.length; j++) {
				BigDecimal tottax = new BigDecimal(0);
				BigDecimal tottaxVE = new BigDecimal(0);
				
				MAllocationLine alloc_line = alloc_lines[j];
				DocLine_Allocation docLine = new DocLine_Allocation(alloc_line, doc);
				doc.setC_BPartner_ID(alloc_line.getC_BPartner_ID());
				
				int inv_id = alloc_line.getC_Invoice_ID();
				if (inv_id <= 0)
					continue;
				MInvoice invoice = null;
				invoice = new MInvoice (ah.getCtx(), alloc_line.getC_Invoice_ID(), ah.get_TrxName());
				if (invoice == null || invoice.getC_Invoice_ID() == 0)
					continue;
				
				alloc_line.setAD_Org_ID(invoice.getAD_Org_ID());
				alloc_line.saveEx();
				/*String sql = 
				  "SELECT i.C_Tax_ID, NVL(SUM(i.TaxBaseAmt),0) AS TaxBaseAmt, NVL(SUM(i.TaxAmt),0) AS TaxAmt, t.Name, t.Rate, t.IsSalesTax "
				 + " FROM LCO_InvoiceWithholding i, C_Tax t "
				+ " WHERE i.C_Invoice_ID = ? AND " +
						 "i.IsCalcOnPayment = 'Y' AND " +
						 "i.IsActive = 'Y' AND " +
						 "i.Processed = 'Y' AND " +
						 "i.C_AllocationLine_ID = ? AND " +
						 "i.C_Tax_ID = t.C_Tax_ID "
				+ "GROUP BY i.C_Tax_ID, t.Name, t.Rate, t.IsSalesTax";
				*/
				
			/**
			 *	Support for get Convertion with current money to money from acct schema
			 * @contribuitor Jorge Colmenarez 2017-08-15 3:15 PM, jcolmenarez@frontuari.com, Frontuari, C.A
			 */
			String sql = 
					  "SELECT i.C_Tax_ID,COALESCE(SUM(i.TaxBaseAmt),0) AS TaxBaseAmt, COALESCE(SUM(i.TaxAmt),0) AS TaxAmt, " +
					  "COALESCE(SUM( currencyconvert(i.TaxBaseAmt,ci.c_currency_id, (SELECT C_Currency_ID FROM C_AcctSchema WHERE AD_Client_ID = i.AD_Client_ID), " +
					  "i.dateacct, ci.c_conversiontype_id, i.ad_client_id, i.ad_org_id) ),0) AS TaxBaseAmtVE, " +
					  "COALESCE(SUM(currencyconvert(i.TaxAmt ,ci.c_currency_id, (SELECT C_Currency_ID FROM C_AcctSchema WHERE AD_Client_ID = i.AD_Client_ID), " +
					  "i.dateacct, ci.c_conversiontype_id, i.ad_client_id, i.ad_org_id)),0) AS TaxAmtVE, t.Name, t.Rate, t.IsSalesTax "
					 + " FROM LCO_InvoiceWithholding i, C_Tax t, C_Invoice ci "
					+ " WHERE i.C_Invoice_ID = ? AND " +
							 "i.IsCalcOnPayment = 'Y' AND " +
							 "i.IsActive = 'Y' AND " +
							 "i.Processed = 'Y' AND " +
							 "i.C_AllocationLine_ID = ? AND " +
							 "i.C_Tax_ID = t.C_Tax_ID AND " +
							 "i.C_Invoice_ID = ci.C_Invoice_ID "
					+ "GROUP BY i.C_Tax_ID, t.Name, t.Rate, t.IsSalesTax ";
				PreparedStatement pstmt = null;
				ResultSet rs = null;
				try
				{
					pstmt = DB.prepareStatement(sql, ah.get_TrxName());
					pstmt.setInt(1, invoice.getC_Invoice_ID());
					pstmt.setInt(2, alloc_line.getC_AllocationLine_ID());
					rs = pstmt.executeQuery();
					while (rs.next()) {
						int tax_ID = rs.getInt(1);
						BigDecimal taxBaseAmt = rs.getBigDecimal(2);
						BigDecimal amount = rs.getBigDecimal(3);
						//String name = rs.getString(4);
						//BigDecimal rate = rs.getBigDecimal(5);
						//boolean salesTax = rs.getString(6).equals("Y") ? true : false;
						BigDecimal taxBaseAmtVE = rs.getBigDecimal(4);
						BigDecimal amountVE = rs.getBigDecimal(5);
						String name = rs.getString(6);
						BigDecimal rate = rs.getBigDecimal(7);
						boolean salesTax = rs.getString(8).equals("Y") ? true : false;
						DocTax taxLine = new DocTax(tax_ID, name, rate, 
								//taxBaseAmt, amount, salesTax);
								taxBaseAmtVE, amountVE, salesTax);
						
						/*if (amount != null && amount.signum() != 0)
						{
							FactLine tl = null;
							if (invoice.isSOTrx()) {
								tl = fact.createLine(null, taxLine.getAccount(DocTax.ACCTTYPE_TaxDue, as),
										as.getC_Currency_ID(), amount, null);
							} else {
								tl = fact.createLine(null, taxLine.getAccount(taxLine.getAPTaxType(), as),
										as.getC_Currency_ID(), null, amount);
							}
							if (tl != null)
								tl.setC_Tax_ID(taxLine.getC_Tax_ID());
							tottax = tottax.add(amount);
						}*/
						if (amountVE != null && amountVE.signum() != 0)
						{
							FactLine tl = null;
							if ((invoice.isSOTrx() && invoice.getC_DocTypeTarget().getDocBaseType().compareTo("ARI")==0)){
							//if ((invoice.isSOTrx() && invoice.getC_DocTypeTarget().getDocBaseType().compareTo("ARI")==0) || (!invoice.isSOTrx() && invoice.getC_DocTypeTarget().getDocBaseType().compareTo("APC")==0)) {
								tl = fact.createLine(docLine, taxLine.getAccount(DocTax.ACCTTYPE_TaxDue, as),
										as.getC_Currency_ID(), amountVE, null);//amount
							
							} 
							//** si es NC proveedor es un iva en compras
							else if (!invoice.isSOTrx() && invoice.getC_DocTypeTarget().getDocBaseType().compareTo("APC")==0){
								tl = fact.createLine(docLine, taxLine.getAccount(taxLine.getAPTaxType(), as),
										as.getC_Currency_ID(), null, amountVE);//amount

							}
							else {
								tl = fact.createLine(docLine, taxLine.getAccount(taxLine.getAPTaxType(), as),
										as.getC_Currency_ID(), null, amountVE);//amount
							}
							if (tl != null)
								tl.setC_Tax_ID(taxLine.getC_Tax_ID());
							tottax = tottax.add(amount);//amount
							tottaxVE = tottaxVE.add(amountVE);//amount
						}
					}
				} catch (Exception e) {
					log.log(Level.SEVERE, sql, e);
					return "Error posting C_InvoiceTax from LCO_InvoiceWithholding";
				} finally {
					DB.close(rs, pstmt);
					rs = null; pstmt = null;
				}
				
				//	Write off		DR
				if (Env.ZERO.compareTo(tottax) != 0)
				{
					// First try to find the WriteOff posting record
					FactLine[] factlines = fact.getLines();
					boolean foundflwriteoff = false;
					for (int ifl = 0; ifl < factlines.length; ifl++) {
						FactLine fl = factlines[ifl];
						if (fl.getAccount().equals(doc.getAccount(Doc.ACCTTYPE_WriteOff, as))) {
							foundflwriteoff = true;
							// old balance = DB - CR
							BigDecimal balamt = fl.getAmtSourceDr().subtract(fl.getAmtSourceCr());
							
							// new balance = old balance +/- tottax
							BigDecimal newbalamt = Env.ZERO;
							if (invoice.isSOTrx())
								newbalamt = balamt.subtract(tottax);
							else if (!invoice.isSOTrx() && invoice.getC_DocTypeTarget().getDocBaseType().compareTo("APC")==0)
								newbalamt = balamt.subtract(tottax);
							else
								newbalamt = balamt.add(tottax);
							if (Env.ZERO.compareTo(newbalamt) == 0) {
								// both zeros, remove the line
								fact.remove(fl);
							} else if (Env.ZERO.compareTo(newbalamt) > 0) {
								fl.setAmtAcct(fl.getC_Currency_ID(), Env.ZERO, newbalamt);
								fl.setAmtSource(fl.getC_Currency_ID(), Env.ZERO, newbalamt);
							} else {
								fl.setAmtAcct(fl.getC_Currency_ID(), newbalamt, Env.ZERO);
								fl.setAmtSource(fl.getC_Currency_ID(), newbalamt, Env.ZERO);
							}
							break;
						}
					}

					if (! foundflwriteoff) {
						// Create a new line
						DocLine_Allocation line = new DocLine_Allocation(alloc_line, doc);
						FactLine fl = null;
						if (invoice.isSOTrx()) {
							fl = fact.createLine (line, doc.getAccount(Doc.ACCTTYPE_WriteOff, as),
									as.getC_Currency_ID(), null, tottaxVE);
						} 

						else {
							fl = fact.createLine (line, doc.getAccount(Doc.ACCTTYPE_WriteOff, as),
									as.getC_Currency_ID(), tottaxVE, null);
						}
						if (fl != null)
							fl.setAD_Org_ID(ah.getAD_Org_ID());
					}
				
				}
				
			}

		}

		return null;
	}


	private String completeInvoiceWithholding(MInvoice inv) {
		
		// Fill DateAcct and DateTrx with final dates from Invoice
		String upd_dates =
			"UPDATE LCO_InvoiceWithholding "
			 + "   SET DateAcct = "
			 + "          (SELECT DateAcct "
			 + "             FROM C_Invoice "
			 + "            WHERE C_Invoice.C_Invoice_ID = LCO_InvoiceWithholding.C_Invoice_ID), "
			 + "       DateTrx = "
			 + "          (SELECT DateInvoiced "
			 + "             FROM C_Invoice "
			 + "            WHERE C_Invoice.C_Invoice_ID = LCO_InvoiceWithholding.C_Invoice_ID) "
			 + " WHERE C_Invoice_ID = ? ";
		int noupddates = DB.executeUpdate(upd_dates, inv.getC_Invoice_ID(), inv.get_TrxName());
		if (noupddates == -1)
			return "Error updating dates on invoice withholding";

		// Set processed for isCalcOnInvoice records
		String upd_proc =
			"UPDATE LCO_InvoiceWithholding "
			 + "   SET Processed = 'Y' "
			 + " WHERE C_Invoice_ID = ? AND IsCalcOnPayment = 'N'";
		int noupdproc = DB.executeUpdate(upd_proc, inv.getC_Invoice_ID(), inv.get_TrxName());
		if (noupdproc == -1)
			return "Error updating processed on invoice withholding";

		return null;
	}

	private String translateWithholdingToTaxes(MInvoice inv) {
		BigDecimal sumit = new BigDecimal(0);
		
		MDocType dt = new MDocType(inv.getCtx(), inv.getC_DocTypeTarget_ID(), inv.get_TrxName());
		String genwh = dt.get_ValueAsString("GenerateWithholding");
		if (genwh == null || genwh.equals("N")) {
			// document configured to not manage withholdings - delete any
			String sqldel = "DELETE FROM LCO_InvoiceWithholding "
				+ " WHERE C_Invoice_ID = ?";
			PreparedStatement pstmtdel = null;
			try
			{
				// Delete previous records generated
				pstmtdel = DB.prepareStatement(sqldel,
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_UPDATABLE, inv.get_TrxName());
				pstmtdel.setInt(1, inv.getC_Invoice_ID());
				int nodel = pstmtdel.executeUpdate();
				log.config("LCO_InvoiceWithholding deleted="+nodel);
			} catch (Exception e) {
				log.log(Level.SEVERE, sqldel, e);
				return "Error creating C_InvoiceTax from LCO_InvoiceWithholding -delete";
			} finally {
				DB.close(pstmtdel);
				pstmtdel = null;
			}
			inv.set_CustomColumn("WithholdingAmt", Env.ZERO);
			
		} else {
			// translate withholding to taxes
			String sql = 
				  "SELECT C_Tax_ID, NVL(SUM(TaxBaseAmt),0) AS TaxBaseAmt, NVL(SUM(TaxAmt),0) AS TaxAmt "
				 + " FROM LCO_InvoiceWithholding "
				+ " WHERE C_Invoice_ID = ? AND IsCalcOnPayment = 'N' AND IsActive = 'Y' "
				+ "GROUP BY C_Tax_ID";
			PreparedStatement pstmt = null;
			ResultSet rs = null;
			try
			{
				pstmt = DB.prepareStatement(sql, inv.get_TrxName());
				pstmt.setInt(1, inv.getC_Invoice_ID());
				rs = pstmt.executeQuery();
				while (rs.next()) {
					MInvoiceTax it = new MInvoiceTax(inv.getCtx(), 0, inv.get_TrxName());
					it.setAD_Org_ID(inv.getAD_Org_ID());
					it.setC_Invoice_ID(inv.getC_Invoice_ID());
					it.setC_Tax_ID(rs.getInt(1));
					it.setTaxBaseAmt(rs.getBigDecimal(2));
					it.setTaxAmt(rs.getBigDecimal(3).negate());
					sumit = sumit.add(rs.getBigDecimal(3));
					if (!it.save())
						return "Error creating C_InvoiceTax from LCO_InvoiceWithholding - save InvoiceTax";
				}
				BigDecimal actualamt = (BigDecimal) inv.get_Value("WithholdingAmt");
				if (actualamt == null)
					actualamt = new BigDecimal(0);
				if (actualamt.compareTo(sumit) != 0 || sumit.signum() != 0) {
					inv.set_CustomColumn("WithholdingAmt", sumit);
					// Subtract to invoice grand total the value of withholdings
					BigDecimal gt = inv.getGrandTotal();
					inv.setGrandTotal(gt.subtract(sumit));
					inv.saveEx();  // need to save here in order to let apply get the right total
				}
				
				if (sumit.signum() != 0) {
					// GrandTotal changed!  If there are payment schedule records they need to be recalculated
					// subtract withholdings from the first installment
					BigDecimal toSubtract = sumit;
					for (MInvoicePaySchedule ips : MInvoicePaySchedule.getInvoicePaySchedule(inv.getCtx(), inv.getC_Invoice_ID(), 0, inv.get_TrxName())) {
						if (ips.getDueAmt().compareTo(toSubtract) >= 0) {
							ips.setDueAmt(ips.getDueAmt().subtract(toSubtract));
							toSubtract = Env.ZERO;
						} else {
							toSubtract = toSubtract.subtract(ips.getDueAmt());
							ips.setDueAmt(Env.ZERO);
						}
						if (!ips.save()) {
							return "Error saving Invoice Pay Schedule subtracting withholdings";
						}
						if (toSubtract.signum() <= 0)
							break;
					}
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, sql, e);
				return "Error creating C_InvoiceTax from LCO_InvoiceWithholding - select InvoiceTax";
			} finally {
				DB.close(rs, pstmt);
				rs = null; pstmt = null;
			}
		}

		return null;
	}

}	//	LCO_Validator