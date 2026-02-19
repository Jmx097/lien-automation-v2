import { LienRecord } from "../types";

export interface EnrichedRecord extends LienRecord {
  amount: string;
}

export type RecordsRow = [
  string, // Site Id
  string, // LienOrReceiveDate
  string, // Amount
  string, // LeadType
  string, // LeadSource
  string, // LiabilityType
  string, // BusinessPersonal
  string, // Company
  string, // FirstName
  string, // LastName
  string, // Street
  string, // City
  string, // State
  string  // Zip
];
