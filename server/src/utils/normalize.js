// server/src/utils/normalize.js

// Normalise un intitulé de colonne -> snake_case sans accents
export function slugifyHeader(h) {
    return String(h || 'col')
      .normalize('NFKD')                 // enlève accents
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-zA-Z0-9]+/g, '_')    // non alphanum -> _
      .replace(/^_+|_+$/g, '')           // supprime _ en début/fin
      .replace(/_{2,}/g, '_')            // fusionne __
      .toLowerCase();
  }
  
  // Essaie de déduire un type SQL compatible DuckDB
  export function guessType(v) {
    if (v === null || v === undefined) return 'VARCHAR';
    if (typeof v === 'number' && Number.isFinite(v)) return 'DOUBLE';
    if (typeof v === 'boolean') return 'BOOLEAN';
  
    const s = String(v).trim();
    if (s === '') return 'VARCHAR';
  
    // Format de date simple
    if (/^\d{4}-\d{2}-\d{2}/.test(s)) return 'TIMESTAMP';
    // Entier
    if (/^-?\d+$/.test(s)) return 'BIGINT';
    // Nombre flottant
    if (/^-?\d+\.\d+$/.test(s)) return 'DOUBLE';
  
    return 'VARCHAR';
  }
  