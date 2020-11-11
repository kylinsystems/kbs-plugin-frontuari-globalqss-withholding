package ve.net.dcs.process;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.List;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MInvoice;
import org.compiere.model.MSysConfig;
import org.compiere.model.Query;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Msg;
import org.globalqss.model.MLCOInvoiceWithholding;
import org.globalqss.model.X_LCO_InvoiceWithholding;

import ve.net.dcs.model.MLVEVoucherWithholding;
import ve.net.dcs.model.VWT_MInvoice;

public class FTU_GenerateWithholdingVouchers extends SvrProcess {
	
	/** The Record						*/
	private int withholdingType = 0;
	private int bPartnerId= 0;
	private int invoiceId = 0;
	private int orgId = 0;
	private Timestamp dateDocFrom;
	private Timestamp dateDocTo;
	private Timestamp dateTrx;
	private int currencyId = 0;
	private int conversiontypeId = 0;
	private String docAction = "DR";
	private int cnt = 0;

	@Override
	protected void prepare() {
		// TODO Auto-generated method stub
		
		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();
			if (para[i].getParameter() == null)
				;			
			else if (name.equals("LCO_WithholdingType_ID"))
				withholdingType = Integer.parseInt(para[i].getParameter().toString());
			else if (name.equals("DateDoc")) {
				dateDocFrom = (Timestamp)para[i].getParameter();
				dateDocTo = (Timestamp)para[i].getParameter_To();
				}
			else if (name.equals("DateTrx"))
				dateTrx = (Timestamp)para[i].getParameter();
			else if (name.equals("C_BPartner_ID"))
				bPartnerId = Integer.parseInt(para[i].getParameter().toString());
			else if (name.equals("C_Invoice_ID"))
				invoiceId = Integer.parseInt(para[i].getParameter().toString());
			else if (name.equals("AD_Org_ID"))
				orgId =Integer.parseInt(para[i].getParameter().toString());
			else if (name.equals("C_Currency_ID"))
				currencyId =Integer.parseInt(para[i].getParameter().toString());
			else if (name.equals("C_ConversionType_ID"))
				conversiontypeId =Integer.parseInt(para[i].getParameter().toString());
			else if (name.equals("DocAction"))
				docAction =para[i].getParameter().toString();
			else
				log.severe("Unknown Parameter: " + name);
		}
		//p_Record_ID =  getRecord_ID(); 
		
	}

	@Override
	protected String doIt() throws Exception {
		
		String msg = Msg.parseTranslation(getCtx(), "@LVE_VoucherWithholding_ID@");
		
		String sqlwt = "select dt.issotrx ,wt.LCO_WithholdingType_ID,wt.Type from LCO_WithholdingType wt "
				+" inner join c_doctype dt on wt.c_doctype_id = dt.c_doctype_id where dt.issotrx='N' and wt.isactive = 'Y' ";
		if(withholdingType>0) 
			sqlwt += " and wt.LCO_WithholdingType_ID="+withholdingType;
		
		PreparedStatement pstmtwt = null;
		ResultSet rswt = null;
		
		try {
			
			pstmtwt= DB.prepareStatement(sqlwt, get_TrxName());
			rswt =  pstmtwt.executeQuery();
			
			while(rswt.next()) {
				int withholdingType = rswt.getInt("LCO_WithholdingType_ID");
				boolean iSOTrx = rswt.getBoolean("IsSOTrx");
				String type = rswt.getString("Type");
				String sql = "" ;
				
				if(type.equalsIgnoreCase("IVA")){
					
					sql = "SELECT i.c_bpartner_id,i.ad_org_id"
							+ " FROM c_invoice i "
							+ " JOIN c_doctype cd  ON cd.c_doctype_id = i.c_doctype_id "
							+ " WHERE NOT EXISTS (SELECT 1 FROM lco_invoicewithholding iw join LVE_VoucherWithholding vo ON iw.LVE_VoucherWithholding_ID = vo.LVE_VoucherWithholding_ID   WHERE iw.c_invoice_id = i.c_invoice_id AND vo.DocStatus IN ('CO','CL') AND iw.lco_withholdingtype_id = "+withholdingType+")"
							+ " AND cd.GenerateWithholding='Y' AND ("
								+ " i.ad_org_id IN  (SELECT DISTINCT Node_ID FROM getnodes("+orgId+",(SELECT AD_Tree_ID FROM AD_Tree WHERE TreeType ='OO' "
								+ "AND AD_Client_ID="+getAD_Client_ID()+"),"+getAD_Client_ID()+") AS N (Parent_ID numeric,Node_ID numeric) " 
								+ " WHERE Parent_ID = "+orgId+") OR i.ad_org_id="+orgId+")"					
							+ " AND i.dateacct BETWEEN '"+dateDocFrom+"' AND '"+dateDocTo+"' ";
					
				}else if(type.equalsIgnoreCase("ISLR")) {					
					sql = "SELECT i.c_bpartner_id,i.ad_org_id"
							+ " FROM c_invoice i "
							+ " JOIN c_doctype cd  ON cd.c_doctype_id = i.c_doctype_id "
							+ " inner join c_invoiceline il on i.c_invoice_id = il.c_invoice_id "
							+ " left join c_charge c on il.c_charge_id = c.c_charge_id "
							+ " left join m_product p on il.m_product_id = p.m_product_id "
							+ " WHERE NOT EXISTS (SELECT 1 FROM lco_invoicewithholding iw join LVE_VoucherWithholding vo ON iw.LVE_VoucherWithholding_ID = vo.LVE_VoucherWithholding_ID   WHERE iw.c_invoice_id = i.c_invoice_id AND vo.DocStatus IN ('CO','CL') AND iw.lco_withholdingtype_id = "+withholdingType+")"
							+ " AND cd.GenerateWithholding='Y'"
							+ " AND (" //i.ad_org_id="+orgId+" "
								+ " i.ad_org_id IN  (SELECT DISTINCT Node_ID FROM getnodes("+orgId+",(SELECT AD_Tree_ID FROM AD_Tree WHERE TreeType ='OO' "
									+ "AND AD_Client_ID="+getAD_Client_ID()+"),"+getAD_Client_ID()+") AS N (Parent_ID numeric,Node_ID numeric) " 
									+ " WHERE Parent_ID = "+orgId+") OR i.ad_org_id="+orgId+")"						
							+ " AND i.dateacct BETWEEN '"+dateDocFrom+"' AND '"+dateDocTo+"' and (c.LCO_WithholdingCategory_ID > 0 OR p.LCO_WithholdingCategory_ID>0)";
					
				}else if(type.equalsIgnoreCase("IAE")) {					
					sql = "SELECT i.c_bpartner_id,i.ad_org_id"
							+ " FROM c_invoice i "
							+ " JOIN c_doctype cd  ON cd.c_doctype_id = i.c_doctype_id "
							+ " inner join C_BPartner bp on bp.c_bpartner_id = i.c_bpartner_id "
							+ " WHERE NOT EXISTS (SELECT 1 FROM lco_invoicewithholding iw join LVE_VoucherWithholding vo ON iw.LVE_VoucherWithholding_ID = vo.LVE_VoucherWithholding_ID   WHERE iw.c_invoice_id = i.c_invoice_id AND vo.DocStatus IN ('CO','CL') AND iw.lco_withholdingtype_id = "+withholdingType+")"
							+ " AND cd.GenerateWithholding='Y' AND ("
								+ " i.ad_org_id IN  (SELECT DISTINCT Node_ID FROM getnodes("+orgId+",(SELECT AD_Tree_ID FROM AD_Tree WHERE TreeType ='OO' "
								+ "AND AD_Client_ID="+getAD_Client_ID()+"),"+getAD_Client_ID()+") AS N (Parent_ID numeric,Node_ID numeric) " 
								+ " WHERE Parent_ID = "+orgId+") OR i.ad_org_id="+orgId+")"					
							+ " AND i.dateacct BETWEEN '"+dateDocFrom+"' AND '"+dateDocTo+"' and bp.LCO_ISIC_ID > 0";
				}				
				
				if(bPartnerId>0)
					sql += " AND i.C_BPartner_ID="+bPartnerId;
				if(invoiceId>0)
					sql += " AND i.C_Invoice_ID="+invoiceId;
				
				sql += " AND i.IsSOTrx= '"+(iSOTrx?"Y":"N")+"' AND i.docstatus IN ('CO','CL') ";
				
				if(MSysConfig.getValue("LVE_GenerateInvoiceWithholdingIsPaid","N",getAD_Client_ID()).compareTo("N")==0){
					sql += " AND i.ISPaid = 'N' ";
				}
				
				sql = sql+" GROUP BY i.c_bpartner_id,i.ad_org_id";
				
				PreparedStatement pstmt = null;
				ResultSet rs = null;
				
				try {
					
					pstmt= DB.prepareStatement(sql, get_TrxName());
					rs =  pstmt.executeQuery();
					int ins = 0;
					int vins = 0;
					while(rs.next()) {
						vins = 0;
						int cBPartnerId = rs.getInt("c_bpartner_id");
						int AD_Org_ID = rs.getInt("AD_Org_ID");
						System.out.println(cBPartnerId);
						
						MLVEVoucherWithholding voucher = new MLVEVoucherWithholding(getCtx(), 0, get_TrxName());
						voucher.setAD_Org_ID(AD_Org_ID);
						voucher.setC_BPartner_ID(cBPartnerId);
						voucher.setDateFrom(dateDocFrom);
						voucher.setDateTo(dateDocTo);
						voucher.setDateTrx(dateTrx);
						voucher.set_ValueOfColumn("DateAcct",dateTrx);
						voucher.setLCO_WithholdingType_ID(withholdingType);
						voucher.setIsSOTrx(iSOTrx);
						voucher.setC_Currency_ID(currencyId);
						voucher.setC_ConversionType_ID(conversiontypeId);
						
						if(invoiceId>0)
							voucher.setC_Invoice_ID(invoiceId);
		
						
						voucher.saveEx(get_TrxName());
						
						MInvoice[] invoices = VWT_MInvoice.getOfBPartnerDateFromDateTo(getCtx(), voucher, get_TrxName());
						
						for (MInvoice mInvoice : invoices) {								
							VWT_MInvoice invoice = new VWT_MInvoice(getCtx(), mInvoice.getC_Invoice_ID(), get_TrxName());
							System.out.println(invoice.getDocumentNo());
							 ins = invoice.recalcWithholdings(voucher);
							 vins +=  ins;
							 cnt += ins;
						}
						
						if(vins>0) {
							if(docAction.equals("CO"))
								processWithholding(voucher);
							
							voucher.saveEx(get_TrxName());
							String WithholdingNo = DB.getSQLValueString(get_TrxName(), "SELECT WithholdingNo FROM LVE_VoucherWithholding WHERE LVE_VoucherWithholding_ID=?", voucher.get_ID());
							//String WithholdingNo = voucher.getWithholdingNo();
							addBufferLog(voucher.get_ID(), new Timestamp(System.currentTimeMillis()), null, msg+": "+WithholdingNo, voucher.get_Table_ID(), voucher.get_ID());
						}else {
							voucher.deleteEx(true, get_TrxName());
						}
					}
				
				}catch(Exception e) {
					throw new AdempiereException(e);
				}finally
				{
					DB.close(rs);
					rs = null;
					pstmt = null;
				}
							
			}
			
		}catch(Exception e) {
			throw new AdempiereException(e);
		}finally
		{
			DB.close(rswt);
			rswt = null;
			pstmtwt = null;
		}
		
		return "Retenciones Generadas: "+cnt;
	}
	
	private void processWithholding(MLVEVoucherWithholding voucher)
	{
		if (docAction.equals("CO")){
			List<MLCOInvoiceWithholding> invoiceW = new Query(voucher.getCtx(), X_LCO_InvoiceWithholding.Table_Name, " LVE_VoucherWithholding_ID = ? ", get_TrxName()).setOnlyActiveRecords(true).setParameters(voucher.get_ID()).list();
			if (invoiceW.size() > 0){
				if(voucher.completeIt().equals(MLVEVoucherWithholding.DOCACTION_Complete))
				{
					DB.executeUpdate("UPDATE LVE_VoucherWithholding SET DocAction='CL',Processed='Y',DocStatus='CO' WHERE LVE_VoucherWithholding_ID = "+voucher.get_ID(),get_TrxName());
				}
				else
				{
					throw new AdempiereException(voucher.getProcessMsg());
				}
			}else{
				throw new AdempiereException("El Comprobante no tiene Línea de Retenciones Asociadas.");
			}
		}
	}

}
