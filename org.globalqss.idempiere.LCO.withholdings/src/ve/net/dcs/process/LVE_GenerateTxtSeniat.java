/******************************************************************************
 * Product: Adempiere ERP & CRM Smart Business Solution                       *
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
 * Contributor(s): Victor Suárez www.dcs.net.ve
 *****************************************************************************/

package ve.net.dcs.process;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MAttachment;
import org.compiere.model.MAttachmentEntry;
import org.compiere.model.MTable;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;

import ve.net.dcs.model.X_LVE_generateTXT;

import java.io.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.logging.Level;

/**
 * Localizacion Venezuela
 * LVE_GenerateTxtSeniat
 * @author Victor Suarez  -  vsuarez@dcs.net.ve  - Double Click Sistemas C.A.
 * 2012
 *
 */
public class LVE_GenerateTxtSeniat extends SvrProcess {

	/**	Organization			*/
	private int	p_AD_Org_ID = 0;	
	/** ValidFrom               */
	private Timestamp 	p_ValidFrom=null;
	/** ValidTo                 */
	private Timestamp   p_ValidTo=null;
	/** TypeOperation           */
	private String      p_TypeOperation=null;
	/** Record_ID               */
	private int p_Record_ID=0;
	/** X_LVE_generateTXT       */
	private int p_LVE_generateTXT_ID = 0;
	
	
	
	protected void prepare() {
		// TODO Auto-generated method stub
	
		p_Record_ID = getRecord_ID();
		p_LVE_generateTXT_ID = p_Record_ID;
		X_LVE_generateTXT generateTXT = new X_LVE_generateTXT(getCtx(), p_LVE_generateTXT_ID, get_TrxName());
		
		p_AD_Org_ID = generateTXT.getAD_Org_ID();
		p_ValidFrom = generateTXT.getValidFrom();
		p_ValidTo = generateTXT.getValidTo();
		p_TypeOperation = generateTXT.getLVE_TypeOperation();
		
		log.log(Level.INFO, "*********  Prepare  **********");
		log.log(Level.INFO, "Parameters: " + "AD_Org_ID: " + p_AD_Org_ID + ", ValidFrom: " + p_ValidFrom + ", ValidTo: " + p_ValidTo + ", TypeOperation: " + p_TypeOperation + ", Record_ID: " + p_Record_ID);
		
		log.log(Level.INFO, "Contexto: " + getCtx().toString());

	}

	protected String doIt() throws Exception {
		// TODO Auto-generated method stub
		String sql="";
		
		String dia, mes , anio;
		Calendar date = new GregorianCalendar();
		dia = Integer.toString(date.get(Calendar.DATE));
		mes = Integer.toString(date.get(Calendar.MONTH) + 1);
		anio = Integer.toString(date.get(Calendar.YEAR));
		String fecha = dia + mes + anio;
		
		String nombreArch="TXT_Seniat";
		String fileNameTXT=nombreArch + p_TypeOperation + fecha + ".txt";
		File archivo=new File(fileNameTXT);
		String contentTXT="";
		
		String tipoOperacion = "";
		
		log.log(Level.INFO, "Fecha: " + fecha);
		log.log(Level.INFO, "Nombre Archivo: " + fileNameTXT);
		
		if (p_TypeOperation.equals("V"))
			tipoOperacion = "VENTAS";
		else tipoOperacion = "COMPRAS";
    
		sql=("SELECT *"
				+ " FROM lve_txtiva " 
				+ " WHERE (lve_txtiva.org = "+p_AD_Org_ID+" OR lve_txtiva.parent_org_id = "+p_AD_Org_ID+")" 
				//+ " lve_txtiva.org = '" + p_AD_Org_ID + "' AND "
				/*	+ " lve_txtiva.org IN  (SELECT DISTINCT Node_ID FROM getnodes("+p_AD_Org_ID+",(SELECT AD_Tree_ID FROM AD_Tree WHERE TreeType ='OO' "
					+ "AND AD_Client_ID="+getAD_Client_ID()+"),"+getAD_Client_ID()+") AS N (Parent_ID numeric,Node_ID numeric) " 
					+ " WHERE Parent_ID = "+p_AD_Org_ID+") OR lve_txtiva.org="+p_AD_Org_ID+")"*/		
				+ " AND (lve_txtiva.fechareten BETWEEN '" + p_ValidFrom + "' AND '"+ p_ValidTo +"') AND"
				+ " lve_txtiva.tipooperacion= '" + p_TypeOperation + "' " 
				);
		//sql = "select * from gg ";
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		log.log(Level.INFO, "SQL: " + sql);
		int cont = 0;
		//BufferedWriter writer = new BufferedWriter(new FileWriter(archivo));
		try {
			pstmt = DB.prepareStatement(sql, get_TrxName());
			rs = pstmt.executeQuery();
			while (rs.next())
			{
			contentTXT+=(rs.getString(1).trim()+"	"
							+rs.getString(2).trim()+"	"
							+rs.getString(3).trim()+"	"
							+rs.getString(4).trim()+"	"
							+rs.getString(5).trim()+"	"
							+rs.getString(6).trim()+"	"
							+rs.getString(7).trim()+"	"
							+rs.getString(8).trim()+"	"
							+rs.getString(9).trim()+"	"
							+rs.getString(10).trim()+"	"
							+rs.getString(11).trim()+"	"
							+rs.getString(12).trim()+"	"
							+rs.getString(13).trim()+"	"
							+rs.getString(14).trim()+"	"
							+rs.getString(15).trim()+"	"
							+rs.getString(16).trim());
			contentTXT+="\n";
			cont ++;
			}
			
			
		}
		catch ( Exception e )
        {
            throw new AdempiereException(e);
        }finally {
        	DB.close(rs,pstmt);
        }
		
		log.info("Contenido: " + contentTXT);
		
		try {
			java.io.FileWriter file = new java.io.FileWriter(archivo);
			java.io.BufferedWriter bw = new java.io.BufferedWriter(file);
			java.io.PrintWriter pw = new java.io.PrintWriter(bw); 
			pw.write(contentTXT);
			pw.close();
			bw.close();	
		}catch (IOException ioe) {
			System.out.println("IOException: " + ioe.getMessage());
            throw new AdempiereException("IOException: "+ioe);
		}
		
		if (contentTXT !="")
		{
			int  AD_Table_ID = MTable.getTable_ID(X_LVE_generateTXT.Table_Name);
			log.log(Level.INFO, "AD_Table_ID: " + AD_Table_ID + " - Record_ID: " + p_Record_ID);
			MAttachment attach =  MAttachment.get(getCtx(),AD_Table_ID,p_Record_ID);
			log.log(Level.INFO, "Contexto: " + getCtx().toString());
		
			if (attach == null ) {
				log.info("attach == null: ");
				log.log(Level.INFO, "AD_Table_ID: " + AD_Table_ID + " - Record_ID: " + p_Record_ID);
				attach = new  MAttachment(getCtx(),AD_Table_ID ,p_Record_ID,get_TrxName());
				attach.addEntry(archivo);
				attach.save();
				log.info("attach.save");
			} else {
				log.info("attach != null: ");
				log.log(Level.INFO, "AD_Table_ID: " + AD_Table_ID + " - Record_ID: " + p_Record_ID);
				int index = (attach.getEntryCount()-1);
				MAttachmentEntry entry = attach.getEntry(index) ;
				String renamed = nombreArch + p_TypeOperation + fecha + "_old" + ".txt";
				entry.setName(renamed);
				attach.save();
				//agrega el nuevo archivo ya q el anterior ha sido renombrado
				attach.addEntry(archivo);
				attach.save();
				}
			return "Archivo Generado y Anexado:  -> " + fileNameTXT + ", "+cont+"Retenciónes Procesadas, Refrescar Ventana y revisar en Anexos.	";
			
		} else
			return "El Archivo no pudo ser Generado porque no hay retenciones de " + tipoOperacion + " para este periodo, desde: " + p_ValidFrom + ", hasta: " + p_ValidTo + ".";
}
}