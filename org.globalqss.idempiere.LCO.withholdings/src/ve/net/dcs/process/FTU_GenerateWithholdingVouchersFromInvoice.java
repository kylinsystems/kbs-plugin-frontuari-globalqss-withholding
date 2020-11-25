package ve.net.dcs.process;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.List;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.Query;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Msg;
import org.globalqss.model.MLCOInvoiceWithholding;
import org.globalqss.model.X_LCO_InvoiceWithholding;

import ve.net.dcs.model.MLVEVoucherWithholding;
import ve.net.dcs.model.VWT_MInvoice;

public class FTU_GenerateWithholdingVouchersFromInvoice extends SvrProcess{

	
	private int withholdingType = 0;
	private Timestamp dateTrx;
	private int currencyId = 0;
	private int conversiontypeId = 0;
	private String docAction = "DR";
	private int cnt = 0;	
	
	
	@Override
	protected void prepare() {
		ProcessInfoParameter[] para = getParameter();
		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();
			if (para[i].getParameter() == null)
				;			
			else if (name.equals("LCO_WithholdingType_ID"))
				withholdingType = Integer.parseInt(para[i].getParameter().toString());
			else if (name.equals("DateDoc")) {
				dateTrx = (Timestamp)para[i].getParameter();
				}
			else if (name.equals("C_Currency_ID"))
				currencyId =Integer.parseInt(para[i].getParameter().toString());
			else if (name.equals("C_ConversionType_ID"))
				conversiontypeId =Integer.parseInt(para[i].getParameter().toString());
			else if (name.equals("DocAction"))
				docAction =para[i].getParameter().toString();
			else
				log.severe("Unknown Parameter: " + name);
		}
	}

	@Override
	protected String doIt() throws Exception {

		String msg = Msg.parseTranslation(getCtx(), "@LVE_VoucherWithholding_ID@");
		
		String sql = "SELECT src.C_Invoice_ID,src.lco_withholdingtype_id FROM FTU_WithholdingVoucherSrc src "
				+ " WHERE EXISTS (SELECT 1 FROM T_Selection ts WHERE trim(ts.ViewID)::numeric = src.C_Invoice_ID AND ts.AD_PInstance_ID="+getAD_PInstance_ID()+")"
				+ " ORDER BY src.C_BPartner_ID, src.AD_Org_ID";
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			
			pstmt= DB.prepareStatement(sql, get_TrxName());
			rs =  pstmt.executeQuery();
			
			int ins = 0;
			int OldBPartner_ID = 0;
			int OldOrg_ID = 0;
			MLVEVoucherWithholding voucher = new MLVEVoucherWithholding(getCtx(), 0, get_TrxName());;
			
			while(rs.next()) {
				int C_Invoice_ID = rs.getInt("C_Invoice_ID");
				withholdingType = rs.getInt("lco_withholdingtype_id");
				if(C_Invoice_ID>0) {
					VWT_MInvoice inv = new VWT_MInvoice(getCtx(),C_Invoice_ID,get_TrxName());
					int AD_Org_ID = inv.getAD_Org_ID();
					int C_BPartner_ID = inv.getC_BPartner_ID();
					if(C_BPartner_ID!=OldBPartner_ID || OldOrg_ID!= AD_Org_ID) {
						
						if(ins>0) {
							if(docAction.equals("CO"))
								processWithholding(voucher);
							
							voucher.saveEx(get_TrxName());
							addBufferLog(voucher.get_ID(), new Timestamp(System.currentTimeMillis()), null, msg+": "+voucher.getWithholdingNo(), voucher.get_Table_ID(), voucher.get_ID());
							cnt += ins;
						}else {
							if(voucher.get_ID()>0)
							voucher.deleteEx(false, get_TrxName());
						}
						
						voucher = new MLVEVoucherWithholding(getCtx(), 0, get_TrxName());
						voucher.setAD_Org_ID(AD_Org_ID);
						voucher.setC_BPartner_ID(C_BPartner_ID);
						voucher.setDateTrx(dateTrx);
						voucher.set_ValueOfColumn("DateAcct",dateTrx);
						voucher.setLCO_WithholdingType_ID(withholdingType);
						voucher.setIsSOTrx(inv.isSOTrx());
						voucher.setC_Currency_ID(currencyId);
						voucher.setC_ConversionType_ID(conversiontypeId);
						voucher.saveEx(get_TrxName());
						OldBPartner_ID=C_BPartner_ID;
						OldOrg_ID=AD_Org_ID;
						ins = 0;
					}
					 ins = inv.recalcWithholdings(voucher);
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
