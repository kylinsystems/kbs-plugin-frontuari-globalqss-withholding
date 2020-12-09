package ve.net.dcs.model;

import java.io.File;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MAllocationHdr;
import org.compiere.model.MBPartner;
import org.compiere.model.MBPartnerLocation;
import org.compiere.model.MBankAccount;
import org.compiere.model.MConversionType;
import org.compiere.model.MDocType;
import org.compiere.model.MFactAcct;
import org.compiere.model.MInvoice;
import org.compiere.model.MLocation;
import org.compiere.model.MOrgInfo;
import org.compiere.model.MPayment;
import org.compiere.model.MPaymentAllocate;
import org.compiere.model.MPriceList;
import org.compiere.model.MTax;
import org.compiere.model.ModelValidationEngine;
import org.compiere.model.ModelValidator;
import org.compiere.model.Query;
import org.compiere.print.MPrintFormat;
import org.compiere.print.ReportEngine;
import org.compiere.process.DocAction;
import org.compiere.process.DocOptions;
import org.compiere.process.DocumentEngine;
import org.compiere.process.ProcessInfo;
import org.compiere.process.ServerProcessCtl;
import org.compiere.util.CLogger;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Msg;
import org.globalqss.model.MLCOInvoiceWithholding;
import org.globalqss.model.X_LCO_WithholdingCalc;
import org.globalqss.model.X_LCO_WithholdingRule;
import org.globalqss.model.X_LCO_WithholdingRuleConf;
import org.globalqss.model.X_LCO_WithholdingType;

public class MLVEVoucherWithholding extends X_LVE_VoucherWithholding implements DocAction,DocOptions{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2297458289364285694L;

	/** Logger */
	private static CLogger log = CLogger.getCLogger(MLVEVoucherWithholding.class);
	private MPaymentAllocate pa = null;
	BigDecimal InvoiceOpenAmt = null; 

	public MLVEVoucherWithholding(Properties ctx, int LVE_VoucherWithholding_ID, String trxName) {
		super(ctx, LVE_VoucherWithholding_ID, trxName);
	}

	public MLVEVoucherWithholding(Properties ctx, ResultSet rs, String trxName) {
		super(ctx, rs, trxName);
	}

	/** Process Message */
	private String m_processMsg = null;

	public String prepareIt() {
		log.info(toString());
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this, ModelValidator.TIMING_BEFORE_PREPARE);
		if (m_processMsg != null)
			return DocAction.STATUS_Invalid;

		// Lines
		MLCOInvoiceWithholding[] lines = getLines(null);
		if (lines.length == 0) {
			m_processMsg = "@NoLines@";
			return DocAction.STATUS_Invalid;
		}

		m_processMsg = ModelValidationEngine.get().fireDocValidate(this, ModelValidator.TIMING_AFTER_PREPARE);
		if (m_processMsg != null)
			return DocAction.STATUS_Invalid;
		m_justPrepared = true;
		return DocAction.STATUS_InProgress;
	}

	public String completeIt() {

		log.info(toString());
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,
		ModelValidator.TIMING_BEFORE_COMPLETE);
		if (m_processMsg != null)
			return DocAction.STATUS_NotApproved;

		if (DOCACTION_Prepare.equals(getDocAction()) || DOCACTION_Re_Activate.equals(getDocAction()))
		{
			setProcessed(false);
			return DocAction.STATUS_InProgress;
		}
		
		if (!m_justPrepared)
		{
			String status = prepareIt();
			if (!DocAction.STATUS_InProgress.equals(status))
				return status;
		}
		
		X_LCO_WithholdingType wt = new X_LCO_WithholdingType(getCtx(), getLCO_WithholdingType_ID(), get_TrxName());
		
		String type=(String)wt.get_Value("type");
		//		Modificado por Jorge Colmenarez, 2017-08-15 2:42 PM jcolmenarez@frontuari.com
		//		Soporte para crear nro de retención para los de tipo ISLR según secuencia de documento.
		
		if (!wt.isSOTrx() && (type.compareTo("IVA")==0 || type.compareTo("ISLR")==0 || type.compareTo("IAE")==0)){
			String WithholdingNo = createWithholdingNo(wt);
			if(WithholdingNo!=null)
				setWithholdingNo(WithholdingNo);
			else if(getWithholdingNo()==null || getWithholdingNo() == "")
				throw new AdempiereException("No se pudo asignar el numero de comprobante");
		}else if (wt.isSOTrx() && type.compareTo("IVA")!=0 && type.compareTo("ISLR")!=0 && type.compareTo("IAE")!=0) {
			String WithholdingNo = createWithholdingNo(wt);
			if(WithholdingNo!=null)
				setWithholdingNo(WithholdingNo);
			else if(getWithholdingNo()==null || getWithholdingNo() == "")
				throw new AdempiereException("No se pudo asignar el numero de comprobante");
		} else if (wt.isSOTrx() && getWithholdingNo() == null && type.compareTo("IVA")==0){
			// m_processMsg = "Asigne un Numero de Comprobante a la Retención";
			// return DocAction.STATUS_Invalid;
			throw new AdempiereException("Asigne un Numero de Comprobante a la Retención");
		}

		MLCOInvoiceWithholding[] lines = getLines(null);
		if (lines.length == 0) {
			m_processMsg = "@NoLines@";
			return DocAction.STATUS_Invalid;
		}

		int C_BankAccount_ID = wt.get_ValueAsInt("C_BankAccount_ID");
		
		if (C_BankAccount_ID == 0) {
			m_processMsg =
			"Debe Establecer un Caja para las Retenciones, Configurador del Sistema LVE_Withholding_BankAccount";
			return DocAction.STATUS_Invalid;
		}

		MBankAccount baccount = new MBankAccount(getCtx(), C_BankAccount_ID, get_TrxName());

		if (baccount.getC_BankAccount_ID() == 0) {
			m_processMsg =
			"Debe Establecer un Caja para las Retenciones, Configurador del Sistema LVE_Withholding_BankAccount";
			return DocAction.STATUS_Invalid;
		}

		FTUWMPayment payment = new FTUWMPayment(getCtx(), 0, get_TrxName());
		payment.setAD_Org_ID(getAD_Org_ID());
		payment.setC_BankAccount_ID(C_BankAccount_ID);
		payment.setDescription("Retencion No: " + getWithholdingNo());
		//	Changed by Jorge Colmenarez 2017-08-28 15:15 
		//	Change getDateTrx to getDateAcct for fixed date accounting into documents 
		payment.setDateAcct(getDateAcct());
		payment.setDateTrx(getDateAcct());
		//	End Jorge Colmenarez
		payment.setTenderType("X");
		payment.setC_BPartner_ID(getC_BPartner_ID());
		/** changed by Adonis Castellanos 12/08/2020 14:48**/
		//MAcctSchema[] m_ass = MAcctSchema.getClientAcctSchema(getCtx(), getAD_Client_ID());
		MBankAccount bank = new MBankAccount(getCtx(),C_BankAccount_ID,get_TrxName());
		int C_Currency_ID =bank.getC_Currency_ID();
		/*if (m_ass.length > 0)
			C_Currency_ID = m_ass[0].getC_Currency_ID();*/
		/** end Adonis **/

		payment.setC_Currency_ID(C_Currency_ID);
		//	Added by Jorge Colmenarez, 2020-08-12 15:42 set ConversionType by Default
		payment.setC_ConversionType_ID(MConversionType.getDefault(getAD_Client_ID()));
		//	End Jorge Colmenarez
		payment.setPayAmt(Env.ZERO);
		payment.setOverUnderAmt(Env.ZERO);
		payment.setWriteOffAmt(Env.ZERO);
		
		int C_Doctype_ID = wt.get_ValueAsInt("C_DocTypeTarget_ID");
		payment.setC_DocType_ID(C_Doctype_ID);
		//	Added By Jorge Colmenarez, 2020-12-08 16:37
		//	Get DocumentNo from Database function
		MDocType dt = new MDocType(getCtx(), C_Doctype_ID, this.get_TrxName());
		String DocumentNo = DB.getSQLValueString(this.get_TrxName(), "SELECT NextDocNo("+dt.getDocNoSequence_ID()+")");
		payment.setDocumentNo(DocumentNo);
		//	End Jorge Colmenarez
		payment.saveEx();
		String sql = null;

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int id_aux = -1;
		for (MLCOInvoiceWithholding mWithholding : lines) {

			mWithholding.set_ValueOfColumn("NroReten", getWithholdingNo());
			mWithholding.set_ValueOfColumn("C_Payment_ID", payment.getC_Payment_ID());
			mWithholding.set_ValueOfColumn("lve_voucherwithholding_id", getLVE_VoucherWithholding_ID());
			mWithholding.setProcessed(true);
			mWithholding.setDateAcct((Timestamp)get_Value("DateAcct"));
			
			if (!mWithholding.save()) {
				throw new AdempiereException("Could not update Withholding Line");
			}
			
			if (id_aux != mWithholding.getC_Invoice_ID()) {
				id_aux = mWithholding.getC_Invoice_ID();
				pa = new MPaymentAllocate(getCtx(), 0, get_TrxName());
				pa.setC_Invoice_ID(mWithholding.getC_Invoice_ID());
				pa.setAD_Org_ID(mWithholding.getAD_Org_ID());
				pa.setAmount(Env.ZERO);
				pa.setC_Payment_ID(payment.getC_Payment_ID());
				//	Set LCO_InvoiceWithholding_ID
				pa.set_ValueOfColumn("LCO_InvoiceWithholding_ID", mWithholding.getLCO_InvoiceWithholding_ID());

				sql = "SELECT invoiceOpen(C_Invoice_ID,0)" // 3 #1
						+ "FROM C_Invoice WHERE C_Invoice_ID=?"; // #4

				pstmt = null;
				rs = null;
				try {
					pstmt = DB.prepareStatement(sql, null);
					pstmt.setInt(1, mWithholding.getC_Invoice_ID());
					rs = pstmt.executeQuery();
					if (rs.next()) {
						InvoiceOpenAmt = rs.getBigDecimal(1);
						if (InvoiceOpenAmt == null)
							InvoiceOpenAmt = Env.ZERO;
					}
				} catch (SQLException e) {
					log.log(Level.SEVERE, sql, e);
					return e.getLocalizedMessage();
				} finally {
					DB.close(rs, pstmt);
					rs = null;
					pstmt = null;
				}

				pa.setInvoiceAmt(InvoiceOpenAmt);
			}

			if (mWithholding.getC_Invoice().getC_DocType().getDocBaseType().equals("ARC") || mWithholding.getC_Invoice().getC_DocType().getDocBaseType().equals("APC"))
				pa.setWriteOffAmt(pa.getWriteOffAmt().add(mWithholding.getTaxAmt().negate()));
			else
				pa.setWriteOffAmt(pa.getWriteOffAmt().add(mWithholding.getTaxAmt()));

			pa.setOverUnderAmt(InvoiceOpenAmt.subtract(pa.getWriteOffAmt()));
			
			pa.saveEx();
		}
		
		if (!payment.processIt(FTUWMPayment.DOCACTION_Complete)) {
			log.warning("Payment Process Failed: " + payment + " - " + payment.getProcessMsg());
			throw new AdempiereException("Payment Process Failed: " + payment + " - " + payment.getProcessMsg());
		}

		payment.saveEx();

		//setC_Payment_ID(payment.getC_Payment_ID());
		DB.executeUpdate("UPDATE LVE_VoucherWithholding SET C_Payment_ID = "+payment.getC_Payment_ID()+" WHERE LVE_VoucherWithholding_ID="+get_ID(),get_TrxName());
		
		// User Validation
		String valid = ModelValidationEngine.get().fireDocValidate(this,
		ModelValidator.TIMING_AFTER_COMPLETE);
		if (valid != null) {
			m_processMsg = valid;
			return DocAction.STATUS_Invalid;
		 }
		//
		setProcessed(true);
		setDocAction(DOCACTION_Close);
		return DocAction.STATUS_Completed;
	}

	public boolean voidIt() {
		log.info(toString());
		// Before Void
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,
		ModelValidator.TIMING_BEFORE_VOID);
		if (m_processMsg != null)
			return false;

		if (getC_Payment_ID() > 0 && getDocStatus().equals(DOCSTATUS_Completed)) {

			//add validation when the invoice has paid don't let me reactivate it or void it
			ValidateInvoicesPayed();
			
			ValidateDeclarationGenerated();
			/*
			 * globalqss - 2317928 - Reactivating/Voiding order must reset
			 * posted
			 */
			MFactAcct.deleteEx(MPayment.Table_ID, getC_Payment_ID(), get_TrxName());

			MLCOInvoiceWithholding[] wth = getLines(null);
			ArrayList<Integer> allhs = new ArrayList<Integer>();
			for (MLCOInvoiceWithholding mlcoInvoiceWithholding : wth) {
				if (mlcoInvoiceWithholding.getC_AllocationLine().getC_AllocationHdr_ID() > 0) {
					
					allhs.add(mlcoInvoiceWithholding.getC_AllocationLine().getC_AllocationHdr_ID());
					List<MLCOInvoiceWithholding> iw = new Query(getCtx(), MLCOInvoiceWithholding.Table_Name, "C_AllocationLine_ID = ?", get_TrxName()).setParameters(mlcoInvoiceWithholding.getC_AllocationLine_ID()).list();
					for (MLCOInvoiceWithholding mlcoInvoiceWithholding2 : iw) {
						mlcoInvoiceWithholding2.setC_AllocationLine_ID(0);
						if (mlcoInvoiceWithholding2.get_ValueAsInt("LVE_VoucherWithholding_ID") != getLVE_VoucherWithholding_ID())
							mlcoInvoiceWithholding2.setProcessed(false);
						mlcoInvoiceWithholding2.saveEx();
					}
					VWT_MInvoice.updateHeaderWithholding(mlcoInvoiceWithholding.getC_Invoice_ID(), get_TrxName());
				}
				else if (mlcoInvoiceWithholding.isProcessed()) {
					mlcoInvoiceWithholding.setProcessed(false);
					mlcoInvoiceWithholding.saveEx();
				}
			}
			
			int oldah = 0;

			for (Integer allh : allhs) {
				if(allh.intValue()>0 && oldah == 0)
				{
					MAllocationHdr ah = new MAllocationHdr(getCtx(), allh.intValue(), get_TrxName());
					MFactAcct.deleteEx(MAllocationHdr.Table_ID, allh.intValue(), get_TrxName());
					ah.delete(true);
					oldah = allh.intValue();	
				}
				else if(allh.intValue()>0 && allh.intValue()!=oldah)
				{
					MAllocationHdr ah = new MAllocationHdr(getCtx(), allh.intValue(), get_TrxName());
					MFactAcct.deleteEx(MAllocationHdr.Table_ID, allh.intValue(), get_TrxName());
					ah.delete(true);
					oldah = allh.intValue();
				}
			}
			
			MAllocationHdr[] allocations = MAllocationHdr.getOfPayment(getCtx(), 
					getC_Payment_ID(), get_TrxName());
			
			if (allocations.length > 0){
				for (MAllocationHdr mAllocationHdr : allocations) {
					MFactAcct.deleteEx(MAllocationHdr.Table_ID, mAllocationHdr.get_ID(), get_TrxName());
					mAllocationHdr.delete(true);
				}
			}

			for (MLCOInvoiceWithholding line : getLines(null)) {
				//line.deleteEx(true);
				line.setC_Payment_ID(0);
				line.save(get_TrxName());
			}
			
			FTUWMPayment pay = new FTUWMPayment(getCtx(), getC_Payment_ID(), get_TrxName());
			pay.delete(true);

		} else {
			throw new AdempiereException("Documento no completado");
		}

		// After Void
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_AFTER_VOID);
		if (m_processMsg != null)
			return false;
		
		setDocStatus(DOCSTATUS_Voided);
		setC_Payment_ID(0);
		m_processMsg = Msg.getMsg(getCtx(), "Voided");
		setProcessed(true);
		setDocAction(DOCACTION_None);
		return true;
	}

	public void ValidateInvoicesPayed() {
		for(MLCOInvoiceWithholding iw:getLines("")) {
			if(iw.getC_Invoice_ID()>0) {
				MInvoice inv = (MInvoice)iw.getC_Invoice();
				if(inv.isPaid())
					throw new AdempiereException("No se puede anular o re-activar el documento porque la factura Nro:"+inv.getDocumentNo()+", ya ha sido pagada");
			}
		}
		
	}
	

	public void ValidateDeclarationGenerated() {
		String sql = "SELECT i.C_Invoice_ID FROM C_InvoiceLine il"
				+ " JOIN C_Invoice i ON il.C_Invoice_ID = i.C_Invoice_ID"
				+ " WHERE i.DocStatus IN ('CO','CL','IP','DR') AND LVE_VoucherWithholding_ID ="+getLVE_VoucherWithholding_ID();
		int C_Invoice_ID = DB.getSQLValueEx(get_TrxName(), sql);
		if(C_Invoice_ID>0) {
			MInvoice inv = new MInvoice(getCtx(),C_Invoice_ID,get_TrxName());
			throw new AdempiereException("No se puede anular o re-activar el documento porque este ya fue declarado en el documento no:"+inv.getDocumentNo());
		}
	}
	
	public String reActiveIt() {
		log.info(toString());

		if (getC_Payment_ID() > 0 && getDocStatus().equals(DOCSTATUS_Completed)) {
			
			//add validation when the invoice has paid don't let me reactivate it or void it
			ValidateInvoicesPayed();
			
			ValidateDeclarationGenerated();
			
			/*
			 * globalqss - 2317928 - Reactivating/Voiding order must reset
			 * posted
			 */
			MFactAcct.deleteEx(MPayment.Table_ID, getC_Payment_ID(), get_TrxName());

			MLCOInvoiceWithholding[] wth = getLines(null);
			ArrayList<Integer> allhs = new ArrayList<Integer>();
			for (MLCOInvoiceWithholding mlcoInvoiceWithholding : wth) {
				if (mlcoInvoiceWithholding.getC_AllocationLine().getC_AllocationHdr_ID() > 0) {
					
					/** Transaction				*/
					allhs.add(mlcoInvoiceWithholding.getC_AllocationLine().getC_AllocationHdr_ID());
					List<MLCOInvoiceWithholding> iw = new Query(getCtx(), MLCOInvoiceWithholding.Table_Name, "C_AllocationLine_ID = ?", get_TrxName()).setParameters(mlcoInvoiceWithholding.getC_AllocationLine_ID()).list();
					for (MLCOInvoiceWithholding mlcoInvoiceWithholding2 : iw) {
						mlcoInvoiceWithholding2.setC_AllocationLine_ID(0);
						if (mlcoInvoiceWithholding2.get_ValueAsInt("LVE_VoucherWithholding_ID") != getLVE_VoucherWithholding_ID())
							mlcoInvoiceWithholding2.setProcessed(false);
						else
							mlcoInvoiceWithholding2.setProcessed(false);
						mlcoInvoiceWithholding2.saveEx();
					}						
					VWT_MInvoice.updateHeaderWithholding(mlcoInvoiceWithholding.getC_Invoice_ID(), get_TrxName());
				}
				else if (mlcoInvoiceWithholding.isProcessed()) {
					mlcoInvoiceWithholding.setProcessed(false);
					mlcoInvoiceWithholding.saveEx();
				}
			}

			int oldah = 0;

			for (Integer allh : allhs) {
				if(allh.intValue()>0 && oldah == 0)
				{
					MAllocationHdr ah = new MAllocationHdr(getCtx(), allh.intValue(), get_TrxName());
					MFactAcct.deleteEx(MAllocationHdr.Table_ID, allh.intValue(), get_TrxName());
					ah.delete(true);
					oldah = allh.intValue();	
				}
				else if(allh.intValue()>0 && allh.intValue()!=oldah)
				{
					MAllocationHdr ah = new MAllocationHdr(getCtx(), allh.intValue(), get_TrxName());
					MFactAcct.deleteEx(MAllocationHdr.Table_ID, allh.intValue(), get_TrxName());
					ah.delete(true);
					oldah = allh.intValue();
				}
			}
			
			MAllocationHdr[] allocations = MAllocationHdr.getOfPayment(getCtx(), 
					getC_Payment_ID(), get_TrxName());
			
			if (allocations.length > 0){
				for (MAllocationHdr mAllocationHdr : allocations) {
					MFactAcct.deleteEx(MAllocationHdr.Table_ID, mAllocationHdr.get_ID(), get_TrxName());
					mAllocationHdr.delete(true);
				}
			}

			FTUWMPayment pay = new FTUWMPayment(getCtx(), getC_Payment_ID(), get_TrxName());
			pay.voidIt();
			pay.saveEx();

		} else {
			throw new AdempiereException("Documento no completado");
		}
		setDocStatus(DOCSTATUS_Drafted);
		setProcessed(false);
		setC_Payment_ID(0);
		return DOCACTION_Re_Activate;
	}

	/**
	 * Get Invoice Lines of Invoice
	 * 
	 * @param whereClause
	 *            starting with AND
	 * @return lines
	 */
	private MLCOInvoiceWithholding[] getLines(String whereClause) {
		String whereClauseFinal = "LVE_VoucherWithholding_ID=? ";
		if (whereClause != null)
			whereClauseFinal += whereClause;
		List<MLCOInvoiceWithholding> list = new Query(getCtx(), MLCOInvoiceWithholding.Table_Name, whereClauseFinal, get_TrxName())
				.setParameters(getLVE_VoucherWithholding_ID())
				.setOrderBy(MLCOInvoiceWithholding.COLUMNNAME_C_Invoice_ID).list();
		return list.toArray(new MLCOInvoiceWithholding[list.size()]);
	} // getLines

	/**
	 * Set the definite document number after completed
	 */
	private String createWithholdingNo(X_LCO_WithholdingType wt) {
		int C_DocType_ID = 0;//= wt.get_ValueAsInt("C_DocType_ID");
		
		String sql = "SELECT wdt.C_DocType_ID FROM LVE_WithholdingDocType wdt "
				+ " WHERE wdt.LCO_WithholdingType_ID="+wt.getLCO_WithholdingType_ID()+" "
						+ " AND wdt.Parent_Org_ID IN (SELECT Parent_Org_ID FROM AD_OrgInfo WHERE AD_Org_ID="+getAD_Org_ID()+")";
		C_DocType_ID = DB.getSQLValue(get_TrxName(), sql);
		
		if(C_DocType_ID<=0)
			throw new AdempiereException("No se pudo encontrar el tipo de documento para la retención");
		
		MDocType dt = new MDocType(getCtx(), C_DocType_ID, get_TrxName());
		
		if(dt.getDocNoSequence_ID()<=0)
			throw new AdempiereException("El tipo de documento para la retención no tiene una secuencia parametrizada");
		
		// Setear secuencia al comprobante si no tiene.
		if(getWithholdingNo() == null || getWithholdingNo() == "") {
			String value = DB.getSQLValueString(get_TrxName(), "SELECT NextDocNo("+dt.getDocNoSequence_ID()+")");//DB.getDocumentNo(dt.getC_DocType_ID(), get_TrxName(), false, this);
			String month = new SimpleDateFormat("MM").format(getDateTrx());
			String year = new SimpleDateFormat("yyyy").format(getDateTrx());
	
			if (value != null){
				value = year + month + value;
				//setWithholdingNo(value);
				DB.executeUpdate("UPDATE LVE_VoucherWithholding SET WithholdingNo='"+value+"' WHERE LVE_VoucherWithholding_ID = "+get_ID(),get_TrxName());
				return value;
			}else {
				throw new AdempiereException("No se pudo encontrar el tipo de documento para la retención");
			}
		}
		return null;
	}

	/**
	 * Before Delete
	 * 
	 * @return true if it can be deleted
	 */
	protected boolean beforeDelete() {
		if (isProcessed()) {
			log.saveError("Error", Msg.getMsg(getCtx(), "CannotDeleteTrx"));
			return false;
		}

		for (MLCOInvoiceWithholding line : getLines(null)) {
			line.deleteEx(true);
		}
		return true;
	} // beforeDelete

	public static BigDecimal CalculateWithholdingsAmt(MInvoice mInvoice) {

		MDocType dt = new MDocType(mInvoice.getCtx(), mInvoice.getC_DocTypeTarget_ID(), mInvoice.get_TrxName());
		BigDecimal taxamttotal = Env.ZERO;
		String genwh = dt.get_ValueAsString("GenerateWithholding");

		if (genwh == null || genwh.equals("N") || genwh.equals(""))
			return Env.ZERO;

		try {
			// Fill variables normally needed
			// BP variables
			MBPartner bp = new MBPartner(mInvoice.getCtx(), mInvoice.getC_BPartner_ID(), mInvoice.get_TrxName());

			Integer bp_isic_int = (Integer) bp.get_Value("LCO_ISIC_ID");
			int bp_isic_id = 0;
			if (bp_isic_int != null)
				bp_isic_id = bp_isic_int.intValue();

			Integer bp_taxpayertype_int = (Integer) bp.get_Value("LCO_TaxPayerType_ID");
			int bp_taxpayertype_id = 0;
			if (bp_taxpayertype_int != null)
				bp_taxpayertype_id = bp_taxpayertype_int.intValue();

			MBPartnerLocation mbpl = new MBPartnerLocation(mInvoice.getCtx(), mInvoice.getC_BPartner_Location_ID(), mInvoice.get_TrxName());
			MLocation bpl = MLocation.get(mInvoice.getCtx(), mbpl.getC_Location_ID(), mInvoice.get_TrxName());
			int bp_city_id = bpl.getC_City_ID();

			// OrgInfo variables
			MOrgInfo oi = MOrgInfo.get(mInvoice.getCtx(), mInvoice.getAD_Org_ID());
			Integer org_isic_int = (Integer) oi.get_Value("LCO_ISIC_ID");
			int org_isic_id = 0;
			if (org_isic_int != null)
				org_isic_id = org_isic_int.intValue();

			Integer org_taxpayertype_int = (Integer) oi.get_Value("LCO_TaxPayerType_ID");
			int org_taxpayertype_id = 0;
			if (org_taxpayertype_int != null)
				org_taxpayertype_id = org_taxpayertype_int.intValue();

			MLocation ol = MLocation.get(mInvoice.getCtx(), oi.getC_Location_ID(), mInvoice.get_TrxName());
			int org_city_id = ol.getC_City_ID();

			// Search withholding types applicable depending on IsSOTrx
			String sqlt = "SELECT LCO_WithholdingType_ID " + " FROM LCO_WithholdingType " + " WHERE IsSOTrx = ? AND IsActive = 'Y' AND AD_Client_ID = ? ";
			PreparedStatement pstmtt = DB.prepareStatement(sqlt, mInvoice.get_TrxName());
			pstmtt.setString(1, mInvoice.getC_DocType().isSOTrx() ? "Y" : "N");
			pstmtt.setInt(2, mInvoice.getAD_Client_ID());
			ResultSet rst = pstmtt.executeQuery();

			while (rst.next()) {
				// For each applicable withholding
				X_LCO_WithholdingType wt = new X_LCO_WithholdingType(mInvoice.getCtx(), rst.getInt(1), mInvoice.get_TrxName());
				X_LCO_WithholdingRuleConf wrc = null;

				// look the conf fields
				String sqlrc = "SELECT * " + " FROM LCO_WithholdingRuleConf " + " WHERE LCO_WithholdingType_ID = ? AND IsActive = 'Y'";
				PreparedStatement pstmtrc = DB.prepareStatement(sqlrc, mInvoice.get_TrxName());
				pstmtrc.setInt(1, wt.getLCO_WithholdingType_ID());
				ResultSet rsrc = pstmtrc.executeQuery();
				if (rsrc.next()) {
					wrc = new X_LCO_WithholdingRuleConf(mInvoice.getCtx(), rsrc, mInvoice.get_TrxName());
				} else {
					rsrc.close();
					pstmtrc.close();
					continue;
				}
				rsrc.close();
				pstmtrc.close();

				// look for applicable rules according to config fields (rule)
				StringBuffer sqlr = new StringBuffer("SELECT LCO_WithholdingRule_ID " + "  FROM LCO_WithholdingRule " + " WHERE LCO_WithholdingType_ID = ? " + "   AND IsActive = 'Y' " + "   AND ValidFrom <= ? ");
				if (wrc.isUseBPISIC())
					sqlr.append(" AND LCO_BP_ISIC_ID = ? ");
				if (wrc.isUseBPTaxPayerType())
					sqlr.append(" AND LCO_BP_TaxPayerType_ID = ? ");
				if (wrc.isUseOrgISIC())
					sqlr.append(" AND LCO_Org_ISIC_ID = ? ");
				if (wrc.isUseOrgTaxPayerType())
					sqlr.append(" AND LCO_Org_TaxPayerType_ID = ? ");
				if (wrc.isUseBPCity())
					sqlr.append(" AND LCO_BP_City_ID = ? ");
				if (wrc.isUseOrgCity())
					sqlr.append(" AND LCO_Org_City_ID = ? ");

				// Add withholding categories of lines
				if (wrc.isUseWithholdingCategory()) {
					// look the conf fields
					String sqlwcs = "SELECT DISTINCT COALESCE (p.LCO_WithholdingCategory_ID, COALESCE (c.LCO_WithholdingCategory_ID, 0)) " + "  FROM C_InvoiceLine il " + "  LEFT OUTER JOIN M_Product p ON (il.M_Product_ID = p.M_Product_ID) "
							+ "  LEFT OUTER JOIN C_Charge c ON (il.C_Charge_ID = c.C_Charge_ID) " + "  WHERE C_Invoice_ID = ? AND il.IsActive='Y'";
					PreparedStatement pstmtwcs = DB.prepareStatement(sqlwcs, mInvoice.get_TrxName());
					pstmtwcs.setInt(1, mInvoice.getC_Invoice_ID());
					ResultSet rswcs = pstmtwcs.executeQuery();
					int i = 0;
					int wcid = 0;
					boolean addedlines = false;
					while (rswcs.next()) {
						wcid = rswcs.getInt(1);
						if (wcid > 0) {
							if (i == 0) {
								sqlr.append(" AND LCO_WithholdingCategory_ID IN (");
								addedlines = true;
							} else {
								sqlr.append(",");
							}
							sqlr.append(wcid);
							i++;
						}
					}
					if (addedlines)
						sqlr.append(") ");
					rswcs.close();
					pstmtwcs.close();
				}

				// Add tax categories of lines
				if (wrc.isUseProductTaxCategory()) {
					// look the conf fields
					String sqlwct = "SELECT DISTINCT COALESCE (p.C_TaxCategory_ID, COALESCE (c.C_TaxCategory_ID, 0)) " + "  FROM C_InvoiceLine il " + "  LEFT OUTER JOIN M_Product p ON (il.M_Product_ID = p.M_Product_ID) " + "  LEFT OUTER JOIN C_Charge c ON (il.C_Charge_ID = c.C_Charge_ID) "
							+ "  WHERE C_Invoice_ID = ? AND il.IsActive='Y'";
					PreparedStatement pstmtwct = DB.prepareStatement(sqlwct, mInvoice.get_TrxName());
					pstmtwct.setInt(1, mInvoice.getC_Invoice_ID());
					ResultSet rswct = pstmtwct.executeQuery();
					int i = 0;
					int wcid = 0;
					boolean addedlines = false;
					while (rswct.next()) {
						wcid = rswct.getInt(1);
						if (wcid > 0) {
							if (i == 0) {
								sqlr.append(" AND C_TaxCategory_ID IN (");
								addedlines = true;
							} else {
								sqlr.append(",");
							}
							sqlr.append(wcid);
							i++;
						}
					}
					if (addedlines)
						sqlr.append(") ");
					rswct.close();
					pstmtwct.close();
				}

				PreparedStatement pstmtr = DB.prepareStatement(sqlr.toString(), mInvoice.get_TrxName());
				int idxpar = 1;
				pstmtr.setInt(idxpar, wt.getLCO_WithholdingType_ID());
				idxpar++;
				pstmtr.setTimestamp(idxpar, mInvoice.getDateInvoiced());
				if (wrc.isUseBPISIC()) {
					idxpar++;
					pstmtr.setInt(idxpar, bp_isic_id);
				}
				if (wrc.isUseBPTaxPayerType()) {
					idxpar++;
					pstmtr.setInt(idxpar, bp_taxpayertype_id);
				}
				if (wrc.isUseOrgISIC()) {
					idxpar++;
					pstmtr.setInt(idxpar, org_isic_id);
				}
				if (wrc.isUseOrgTaxPayerType()) {
					idxpar++;
					pstmtr.setInt(idxpar, org_taxpayertype_id);
				}
				if (wrc.isUseBPCity()) {
					idxpar++;
					pstmtr.setInt(idxpar, bp_city_id);
				}
				if (wrc.isUseOrgCity()) {
					idxpar++;
					pstmtr.setInt(idxpar, org_city_id);
				}

				ResultSet rsr = pstmtr.executeQuery();
				while (rsr.next()) {
					// for each applicable rule
					X_LCO_WithholdingRule wr = new X_LCO_WithholdingRule(mInvoice.getCtx(), rsr.getInt(1), mInvoice.get_TrxName());

					// bring record for withholding calculation
					X_LCO_WithholdingCalc wc = new X_LCO_WithholdingCalc(mInvoice.getCtx(), wr.getLCO_WithholdingCalc_ID(), mInvoice.get_TrxName());
					if (wc.getLCO_WithholdingCalc_ID() == 0) {
						continue;
					}

					// bring record for tax
					MTax tax = new MTax(mInvoice.getCtx(), wc.getC_Tax_ID(), mInvoice.get_TrxName());

					// calc base
					// apply rule to calc base
					BigDecimal base = null;

					if (wc.getBaseType() == null) {
						return new BigDecimal(-1);
					} else if (wc.getBaseType().equals(X_LCO_WithholdingCalc.BASETYPE_Document)) {
						base = mInvoice.getTotalLines();
					} else if (wc.getBaseType().equals(X_LCO_WithholdingCalc.BASETYPE_Line)) {
						String sqllca;
						if (wrc.isUseWithholdingCategory() && wrc.isUseProductTaxCategory()) {
							// base = lines of the withholding category and tax
							// category
							sqllca = "SELECT SUM (LineNetAmt) " + "  FROM C_InvoiceLine il " + " WHERE IsActive='Y' AND C_Invoice_ID = ? " + "   AND (   EXISTS ( " + "              SELECT 1 " + "                FROM M_Product p " + "               WHERE il.M_Product_ID = p.M_Product_ID "
									+ "                 AND p.C_TaxCategory_ID = ? " + "                 AND p.LCO_WithholdingCategory_ID = ?) " + "        OR EXISTS ( " + "              SELECT 1 " + "                FROM C_Charge c " + "               WHERE il.C_Charge_ID = c.C_Charge_ID "
									+ "                 AND c.C_TaxCategory_ID = ? " + "                 AND c.LCO_WithholdingCategory_ID = ?) " + "       ) ";
						} else if (wrc.isUseWithholdingCategory()) {
							// base = lines of the withholding category
							sqllca = "SELECT SUM (LineNetAmt) " + "  FROM C_InvoiceLine il " + " WHERE IsActive='Y' AND C_Invoice_ID = ? " + "   AND (   EXISTS ( " + "              SELECT 1 " + "                FROM M_Product p " + "               WHERE il.M_Product_ID = p.M_Product_ID "
									+ "                 AND p.LCO_WithholdingCategory_ID = ?) " + "        OR EXISTS ( " + "              SELECT 1 " + "                FROM C_Charge c " + "               WHERE il.C_Charge_ID = c.C_Charge_ID "
									+ "                 AND c.LCO_WithholdingCategory_ID = ?) " + "       ) ";
						} else if (wrc.isUseProductTaxCategory()) {
							// base = lines of the product tax category
							sqllca = "SELECT SUM (LineNetAmt) " + "  FROM C_InvoiceLine il " + " WHERE IsActive='Y' AND C_Invoice_ID = ? " + "   AND (   EXISTS ( " + "              SELECT 1 " + "                FROM M_Product p " + "               WHERE il.M_Product_ID = p.M_Product_ID "
									+ "                 AND p.C_TaxCategory_ID = ?) " + "        OR EXISTS ( " + "              SELECT 1 " + "                FROM C_Charge c " + "               WHERE il.C_Charge_ID = c.C_Charge_ID " + "                 AND c.C_TaxCategory_ID = ?) " + "       ) ";
						} else {
							// base = all lines
							sqllca = "SELECT SUM (LineNetAmt) " + "  FROM C_InvoiceLine il " + " WHERE IsActive='Y' AND C_Invoice_ID = ? ";
						}

						PreparedStatement pstmtlca = DB.prepareStatement(sqllca, mInvoice.get_TrxName());
						pstmtlca.setInt(1, mInvoice.getC_Invoice_ID());
						if (wrc.isUseWithholdingCategory() && wrc.isUseProductTaxCategory()) {
							pstmtlca.setInt(2, wr.getC_TaxCategory_ID());
							pstmtlca.setInt(3, wr.getLCO_WithholdingCategory_ID());
							pstmtlca.setInt(4, wr.getC_TaxCategory_ID());
							pstmtlca.setInt(5, wr.getLCO_WithholdingCategory_ID());
						} else if (wrc.isUseWithholdingCategory()) {
							pstmtlca.setInt(2, wr.getLCO_WithholdingCategory_ID());
							pstmtlca.setInt(3, wr.getLCO_WithholdingCategory_ID());
						} else if (wrc.isUseProductTaxCategory()) {
							pstmtlca.setInt(2, wr.getC_TaxCategory_ID());
							pstmtlca.setInt(3, wr.getC_TaxCategory_ID());
						} else {
							; // nothing
						}
						ResultSet rslca = pstmtlca.executeQuery();
						if (rslca.next())
							base = rslca.getBigDecimal(1);
						rslca.close();
						pstmtlca.close();
					} else if (wc.getBaseType().equals(X_LCO_WithholdingCalc.BASETYPE_Tax)) {
						// if specific tax
						if (wc.getC_BaseTax_ID() != 0) {
							// base = value of specific tax
							String sqlbst = "SELECT SUM(TaxAmt) " + " FROM C_InvoiceTax " + " WHERE IsActive='Y' AND C_Invoice_ID = ? " + "   AND C_Tax_ID = ?";
							PreparedStatement pstmtbst = DB.prepareStatement(sqlbst, mInvoice.get_TrxName());
							pstmtbst.setInt(1, mInvoice.getC_Invoice_ID());
							pstmtbst.setInt(2, wc.getC_BaseTax_ID());
							ResultSet rsbst = pstmtbst.executeQuery();
							if (rsbst.next())
								base = rsbst.getBigDecimal(1);
							rsbst.close();
							pstmtbst.close();
						} else {
							// not specific tax
							// base = value of all taxes
							String sqlbsat = "SELECT SUM(TaxAmt) " + " FROM C_InvoiceTax " + " WHERE IsActive='Y' AND C_Invoice_ID = ? ";
							PreparedStatement pstmtbsat = DB.prepareStatement(sqlbsat, mInvoice.get_TrxName());
							pstmtbsat.setInt(1, mInvoice.getC_Invoice_ID());
							ResultSet rsbsat = pstmtbsat.executeQuery();
							if (rsbsat.next())
								base = rsbsat.getBigDecimal(1);
							rsbsat.close();
							pstmtbsat.close();
						}
					}

					// if base between thresholdmin and thresholdmax inclusive
					// if thresholdmax = 0 it is ignored
					if (base != null && base.compareTo(Env.ZERO) != 0 && base.compareTo(wc.getThresholdmin()) >= 0 && (wc.getThresholdMax() == null || wc.getThresholdMax().compareTo(Env.ZERO) == 0 || base.compareTo(wc.getThresholdMax()) <= 0) && tax.getRate() != null
							&& tax.getRate().compareTo(Env.ZERO) != 0) {

						int stdPrecision = MPriceList.getStandardPrecision(mInvoice.getCtx(), mInvoice.getM_PriceList_ID());
						BigDecimal taxamt = tax.calculateTax(base, false, stdPrecision);
						if (wc.getAmountRefunded() != null && wc.getAmountRefunded().compareTo(Env.ZERO) > 0) {
							taxamt = taxamt.subtract(wc.getAmountRefunded());
						}

						taxamttotal = taxamttotal.add(taxamt);
					}
				} // while each applicable rule

			} // while type

			rst.close();
			pstmtt.close();
		} catch (SQLException e) {
			return new BigDecimal(-1);
		}

		return taxamttotal;
	}

	@Override
	public boolean processIt(String processAction) throws Exception {
		m_processMsg = null;
		DocumentEngine engine = new DocumentEngine (this, getDocStatus());
		return engine.processIt (processAction, getDocAction());
	}

	/**	Just Prepared Flag			*/
	private boolean		m_justPrepared = false;
	
	@Override
	public boolean unlockIt() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean invalidateIt() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean approveIt() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rejectIt() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean closeIt() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean reverseCorrectIt() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean reverseAccrualIt() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean reActivateIt() {
		log.info(toString());
		// Before reActivate
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_BEFORE_REACTIVATE);
		if (m_processMsg != null)
			return false;

		if (getC_Payment_ID() > 0 && getDocStatus().equals(DOCSTATUS_Completed)) {
			

			//add validation when the invoice has paid don't let me reactivate it or void it
			ValidateInvoicesPayed();
			
			ValidateDeclarationGenerated();
			
			/*
			 * globalqss - 2317928 - Reactivating/Voiding order must reset
			 * posted
			 */
			MFactAcct.deleteEx(MPayment.Table_ID, getC_Payment_ID(), get_TrxName());

			MLCOInvoiceWithholding[] wth = getLines(null);
			ArrayList<Integer> allhs = new ArrayList<Integer>();
			for (MLCOInvoiceWithholding mlcoInvoiceWithholding : wth) {
				if (mlcoInvoiceWithholding.getC_AllocationLine().getC_AllocationHdr_ID() > 0) {
					
					/** Transaction				*/
					allhs.add(mlcoInvoiceWithholding.getC_AllocationLine().getC_AllocationHdr_ID());
					List<MLCOInvoiceWithholding> iw = new Query(getCtx(), MLCOInvoiceWithholding.Table_Name, "C_AllocationLine_ID = ?", get_TrxName()).setParameters(mlcoInvoiceWithholding.getC_AllocationLine_ID()).list();
					for (MLCOInvoiceWithholding mlcoInvoiceWithholding2 : iw) {
						mlcoInvoiceWithholding2.setC_AllocationLine_ID(0);
						if (mlcoInvoiceWithholding2.get_ValueAsInt("LVE_VoucherWithholding_ID") != getLVE_VoucherWithholding_ID())
							mlcoInvoiceWithholding2.setProcessed(false);
						else
							mlcoInvoiceWithholding2.setProcessed(false);
						mlcoInvoiceWithholding2.saveEx();
					}		
					VWT_MInvoice.updateHeaderWithholding(mlcoInvoiceWithholding.getC_Invoice_ID(), get_TrxName());
				}
				else if (mlcoInvoiceWithholding.isProcessed()) {
					mlcoInvoiceWithholding.setProcessed(false);
					mlcoInvoiceWithholding.saveEx();
				}
			}

			for (Integer allh : allhs) {
				MAllocationHdr ah = new MAllocationHdr(getCtx(), allh.intValue(), get_TrxName());
				MFactAcct.deleteEx(MAllocationHdr.Table_ID, allh.intValue(), get_TrxName());
				ah.delete(true);
			}
			
			MAllocationHdr[] allocations = MAllocationHdr.getOfPayment(getCtx(), 
					getC_Payment_ID(), get_TrxName());
			
			if (allocations.length > 0){
				for (MAllocationHdr mAllocationHdr : allocations) {
					MFactAcct.deleteEx(MAllocationHdr.Table_ID, mAllocationHdr.get_ID(), get_TrxName());
					mAllocationHdr.delete(true);
				}
			}

			FTUWMPayment pay = new FTUWMPayment(getCtx(), getC_Payment_ID(), get_TrxName());
			pay.voidIt();
			pay.saveEx();
		} else {
			throw new AdempiereException("Documento no completado");
		}
		//	Clear Payment Reference
		MLCOInvoiceWithholding[] wth = getLines(null);
		for(MLCOInvoiceWithholding line : wth)
		{
			line.setC_Payment_ID(0);
			line.saveEx();
		}
		setC_Payment_ID(0);
		
		// After reActivate
		m_processMsg = ModelValidationEngine.get().fireDocValidate(this,ModelValidator.TIMING_AFTER_REACTIVATE);
		if (m_processMsg != null)
			return false;
		
		setDocAction(DOCACTION_Complete);
		setProcessed(false);
		return true;
	}

	@Override
	public String getSummary() {
		// TODO Auto-generated method stub
		//return null;
		return "Ok";
	}

	@Override
	public String getDocumentInfo() {
		MDocType dt = MDocType.get(getCtx(),getC_DocType_ID());
		return dt.getNameTrl() + " " + getDocumentNo();
	}

	@Override
	public File createPDF() {
		try
		{
			File temp = File.createTempFile(get_TableName()+get_ID()+"_", ".pdf");
			return createPDF (temp);
		}
		catch (Exception e)
		{
			log.severe("Could not create PDF - " + e.getMessage());
		}
		return null;
	}

	@Override
	public String getProcessMsg()
	{
		return m_processMsg;
	}	//	getProcessMsg

	@Override
	public int getDoc_User_ID() {

		return getCreatedBy();
	}

	@Override
	public int getC_Currency_ID() {
		Integer ii = (Integer)get_Value(COLUMNNAME_C_Currency_ID);
		if (ii == null)
			 return 0;
		return ii.intValue();
	}

	@Override
	public BigDecimal getApprovalAmt() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int customizeValidActions(String docStatus, Object processing,
			String orderType, String isSOTrx, int AD_Table_ID,
			String[] docAction, String[] options, int index) {
		if (options == null)
			throw new IllegalArgumentException("Option array parameter is null");
		if (docAction == null)
			throw new IllegalArgumentException("Doc action array parameter is null");

		// If a document is drafted or invalid, the users are able to complete, prepare or void
		if (docStatus.equals(DocumentEngine.STATUS_Drafted) || docStatus.equals(DocumentEngine.STATUS_Invalid)) {
			options[index++] = DocumentEngine.ACTION_Complete;
			options[index++] = DocumentEngine.ACTION_Prepare;
			options[index++] = DocumentEngine.ACTION_Void;

			// If the document is already completed, we also want to be able to reactivate or void it instead of only closing it
		} else if (docStatus.equals(DocumentEngine.STATUS_Completed)) {
			options[index++] = DocumentEngine.ACTION_Void;
			options[index++] = DocumentEngine.ACTION_ReActivate;
		}

		return index;
	}

	public void setProcessMessage(String processMsg)
	{
		m_processMsg = processMsg;
	}
	/**
	 * 	Create PDF file
	 *	@param file output file
	 *	@return file if success
	 */
	public File createPDF (File file)
	{
		ReportEngine re = ReportEngine.get (getCtx(), ReportEngine.ORDER, getLVE_VoucherWithholding_ID(), get_TrxName());
		if (re == null)
			return null;
		MPrintFormat format = re.getPrintFormat();
		// We have a Jasper Print Format
		// ==============================
		if(format.getJasperProcess_ID() > 0)
		{
			ProcessInfo pi = new ProcessInfo ("", format.getJasperProcess_ID());
			pi.setRecord_ID ( getLVE_VoucherWithholding_ID());
			pi.setIsBatch(true);
			
			ServerProcessCtl.process(pi, null);
			
			return pi.getPDFReport();
		}
		// Standard Print Format (Non-Jasper)
		// ==================================
		return re.getPDF(file);
	}	//	createPDF
}
