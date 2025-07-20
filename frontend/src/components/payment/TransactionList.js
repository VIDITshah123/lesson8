import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Chip,
  CircularProgress,
  Alert,
  TextField,
  InputAdornment
} from '@mui/material';
import { 
  Search as SearchIcon,
  Check as CheckIcon,
  QueryBuilder as PendingIcon
} from '@mui/icons-material';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';
import { format } from 'date-fns';

const TransactionList = () => {
  const [transactions, setTransactions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const [searchQuery, setSearchQuery] = useState('');
  const [totalCount, setTotalCount] = useState(0);
  
  const { token } = useAuth();
  
  // Fetch transactions from the API
  const fetchTransactions = async (searchTerm = '', pageNumber = 0, pageSize = 10) => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await axios.get('/api/payment/transactions', {
        params: {
          search: searchTerm,
          page: pageNumber,
          limit: pageSize
        },
        headers: {
          'Authorization': `Bearer ${token}`
        }
      });
      
      setTransactions(response.data.transactions);
      setTotalCount(response.data.totalCount || response.data.transactions.length);
    } catch (err) {
      console.error('Error fetching transactions:', err);
      setError(
        err.response?.data?.error || 
        'Failed to fetch transaction data. Please try again.'
      );
    } finally {
      setLoading(false);
    }
  };
  
  // Initial load
  useEffect(() => {
    fetchTransactions(searchQuery, page, rowsPerPage);
  }, [token, page, rowsPerPage]);
  
  // Handle page change
  const handleChangePage = (event, newPage) => {
    setPage(newPage);
  };
  
  // Handle rows per page change
  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };
  
  // Handle search
  const handleSearch = (event) => {
    const query = event.target.value;
    setSearchQuery(query);
    
    // Debounce search
    const timeoutId = setTimeout(() => {
      setPage(0);
      fetchTransactions(query, 0, rowsPerPage);
    }, 500);
    
    return () => clearTimeout(timeoutId);
  };
  
  // Format date
  const formatDate = (dateString) => {
    try {
      return format(new Date(dateString), 'PPpp');
    } catch (e) {
      return dateString;
    }
  };
  
  if (loading && transactions.length === 0) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="200px">
        <CircularProgress />
      </Box>
    );
  }
  
  return (
    <Paper sx={{ p: 2, width: '100%' }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Typography variant="h6" component="h2">
          Payment Transactions
        </Typography>
        
        <TextField
          variant="outlined"
          size="small"
          placeholder="Search transactions..."
          value={searchQuery}
          onChange={handleSearch}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon fontSize="small" />
              </InputAdornment>
            ),
          }}
          sx={{ width: '300px' }}
        />
      </Box>
      
      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      
      <TableContainer>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Transaction ID</TableCell>
              <TableCell>Reference</TableCell>
              <TableCell>User</TableCell>
              <TableCell>Amount</TableCell>
              <TableCell>QR Code</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Date</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {transactions.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} align="center">
                  {loading ? "Loading..." : "No transactions found"}
                </TableCell>
              </TableRow>
            ) : (
              transactions.map((transaction) => (
                <TableRow key={transaction.id} hover>
                  <TableCell>{transaction.id}</TableCell>
                  <TableCell>{transaction.transactionRef}</TableCell>
                  <TableCell>
                    {transaction.user ? `${transaction.user.firstName} ${transaction.user.lastName}` : 'Unknown'}
                  </TableCell>
                  <TableCell>{transaction.qrCode?.amount || 'N/A'}</TableCell>
                  <TableCell>{transaction.qrCode?.name || 'Unknown'}</TableCell>
                  <TableCell>
                    <Chip
                      size="small"
                      icon={transaction.verified ? <CheckIcon /> : <PendingIcon />}
                      label={transaction.verified ? "Verified" : "Pending"}
                      color={transaction.verified ? "success" : "default"}
                    />
                  </TableCell>
                  <TableCell>{formatDate(transaction.createdAt)}</TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </TableContainer>
      
      <TablePagination
        rowsPerPageOptions={[5, 10, 25]}
        component="div"
        count={totalCount}
        rowsPerPage={rowsPerPage}
        page={page}
        onPageChange={handleChangePage}
        onRowsPerPageChange={handleChangeRowsPerPage}
      />
    </Paper>
  );
};

export default TransactionList;
