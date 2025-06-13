import axios from 'axios';
import { useEffect, useRef, useState } from 'react';
import '../styles/chatbot.css';

const Chatbot = () => {
  const [messages, setMessages] = useState([
    { 
      type: 'assistant', 
      content: 'Hello! I can help with employee productivity analytics and also chat about anything else. Ask me questions about your data or let\'s have a casual conversation!' 
    }
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [apiError, setApiError] = useState(null);
  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);

  // Auto-scroll to bottom of messages
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // Focus input on load
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  // Check API key on mount
 

  // Check if the input is a data analytics query
  const isDataQuery = (text) => {
    const dataKeywords = [
      'show', 'display', 'list', 'find', 'get', 'fetch', 'retrieve', 'select',
      'teams', 'team', 'employees', 'employee', 'productivity', 'performance',
      'data', 'report', 'metrics', 'statistics', 'analytics', 'numbers',
      'department', 'departments', 'group', 'groups', 'active', 'inactive',
      'above', 'below', 'greater', 'less', 'than', 'percent', '%',
      'top', 'bottom', 'best', 'worst', 'high', 'low', 'average',
      'compare', 'comparison', 'analysis', 'trend', 'trends'
    ];
    
    const lowercaseText = text.toLowerCase();
    
    // Check if it contains data-related keywords
    const hasDataKeyword = dataKeywords.some(keyword => 
      lowercaseText.includes(keyword)
    );
    
    // Additional check for specific data query patterns
    const dataPatterns = [
      /give me.*data/i,
      /show.*with.*productivity/i,
      /list.*employees/i,
      /teams.*below/i,
      /employees.*above/i,
      /productivity.*percent/i,
      /performance.*data/i
    ];
    
    const hasDataPattern = dataPatterns.some(pattern => pattern.test(text));
    
    return hasDataKeyword || hasDataPattern;
  };

  // Make Gemini API call through backend
  const callGeminiAPI = async (prompt, retries = 2) => {
    try {
      const response = await fetch('http://127.0.0.1:3000/api/gemini/generate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          prompt,
          retries
        })
      });

      const result = await response.json();
      
      if (!response.ok) {
        const errorMsg = result.detail || 'Failed to get response from Gemini API';
        console.error('API Error:', errorMsg);
        throw new Error(errorMsg);
      }

      if (!result.success) {
        // Handle content filtering and safety issues gracefully
        if (result.message) {
          if (result.message.includes('dangerous')) {
            return "I apologize, but I cannot generate content that may be dangerous. Please try a different request.";
          } else if (result.message.includes('safety') || result.message.includes('blocked')) {
            return "I apologize, but I cannot process that request due to content safety guidelines. Please rephrase your question.";
          }
        }
        throw new Error(result.message || 'Failed to generate content');
      }

      return result.response;
      
    } catch (error) {
      console.error('Gemini API error:', error);
      throw error;
    }
  };

  // Generate conversational response using Gemini AI
  const generateConversationalResponse = async (question) => {
    const prompt = `You are a friendly, helpful productivity analytics assistant named "Smart Analytics Assistant". 

The user just said: "${question}"

You specialize in employee productivity analytics but can also have casual conversations about anything.

Respond naturally and conversationally. You can:
- Answer questions about anything
- Tell fresh, unique jokes (workplace-appropriate, never repeat jokes)
- Have casual conversations on any topic
- Provide helpful information
- Be engaging and personable

IMPORTANT: 
- Always provide fresh, new responses - never repeat content
- If asked for jokes, make them unique each time
- Keep responses under 150 words
- Be friendly and helpful
- Respond naturally as if having a real conversation

Please respond to the user's message now:`;

    try {
      const response = await callGeminiAPI(prompt);
      return response;
    } catch (error) {
      console.error('Failed to get conversational response:', error);
      
      // Provide specific error messages based on the error type
      if (error.message.includes('API key')) {
        return "🔑 I'm having authentication issues with my AI service. Please check the configuration.";
      } else if (error.message.includes('quota')) {
        return "📊 I've reached my usage limit for now. Please try again in a bit!";
      } else if (error.message.includes('network') || error.message.includes('timed out')) {
        return "🌐 I'm having trouble connecting to the internet. Please check your connection and try again.";
      } else {
        return "🤖 I'm experiencing technical difficulties. Let me try to help you in a different way - what would you like to know?";
      }
    }
  };

  // Generate report using Gemini AI
  const generateReport = async (question, data) => {
    const dataString = JSON.stringify(data.slice(0, 10), null, 2);
    
    const prompt = `Analyze this employee productivity data and create a professional business report.

User Query: "${question}"
Data (${data.length} total records):
${dataString}

Generate a concise business intelligence report that:
1. Summarizes key findings in 2-3 bullet points
2. Identifies patterns or trends
3. Provides actionable insights
4. Uses specific numbers from the data
5. Keeps it under 150 words
6. Uses professional business language

Format as a brief executive summary.`;

    try {
      const report = await callGeminiAPI(prompt);
      return cleanAIResponse(report);
    } catch (error) {
      console.error('Failed to generate AI report:', error);
      return generateFallbackReport(question, data);
    }
  };

  // Clean AI response
  const cleanAIResponse = (response) => {
    if (!response) return "Analysis completed.";
    
    let cleaned = response
      .replace(/\*\*(.*?)\*\*/g, '**$1**')
      .replace(/^\*\s+/gm, '• ')
      .replace(/^-\s+/gm, '• ')
      .trim();
    
    return cleaned;
  };

  // Fallback report generation
  const generateFallbackReport = (question, data) => {
    if (!data || data.length === 0) {
      return "No data available for analysis.";
    }

    const totalRecords = data.length;
    const columns = Object.keys(data[0] || {});
    
    let report = `**Executive Summary**\n\n`;
    report += `• **${totalRecords} records** found matching your query\n`;
    
    if (question.toLowerCase().includes('below') && question.toLowerCase().includes('30')) {
      report += `• **${totalRecords} teams** have productivity levels below 30%\n`;
      report += `• This represents potential areas requiring immediate attention and improvement strategies\n`;
    } else if (columns.some(col => col.toLowerCase().includes('productivity'))) {
      report += `• Productivity metrics analysis available across ${totalRecords} entities\n`;
    } else if (columns.some(col => col.toLowerCase().includes('team'))) {
      report += `• Team-based analysis covering ${totalRecords} organizational units\n`;
    }
    
    report += `\n**Recommendation:** Review detailed data below to identify specific improvement opportunities.`;
    
    return report;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const userMessage = { type: 'user', content: input };
    setMessages(prev => [...prev, userMessage]);
    const currentInput = input;
    setInput('');
    setIsLoading(true);

    try {
      if (isDataQuery(currentInput)) {
        // Handle data analytics queries
        try {
          const response = await axios.post('http://127.0.0.1:3000/api/query', { 
            question: currentInput 
          }, {
            timeout: 30000,
            headers: {
              'Content-Type': 'application/json'
            }
          });
          
          if (response.data.error) {
            setMessages(prev => [...prev, { 
              type: 'assistant', 
              content: `❌ ${response.data.error}`, 
              isError: true 
            }]);
          } else if (response.data.results && response.data.results.length > 0) {
            // Generate AI report for the data
            const aiReport = await generateReport(currentInput, response.data.results);
            
            setMessages(prev => [...prev, { 
              type: 'assistant', 
              content: aiReport,
              results: response.data.results,
              isReport: true
            }]);
          } else {
            setMessages(prev => [...prev, { 
              type: 'assistant', 
              content: response.data.response || "No data found for your query. Try rephrasing your question."
            }]);
          }
        } catch (axiosError) {
          console.error('Data API error:', axiosError);
          let errorMessage = "⚠️ Unable to process your data request. ";
          
          if (axiosError.code === 'ECONNABORTED') {
            errorMessage += "The query is taking too long. Please try a simpler question.";
          } else if (axiosError.code === 'ECONNREFUSED') {
            errorMessage += "Cannot connect to the data service. Please ensure the backend server is running.";
          } else if (!navigator.onLine) {
            errorMessage += "Please check your internet connection.";
          } else {
            errorMessage += "Please try again or rephrase your question.";
          }
          
          setMessages(prev => [...prev, { 
            type: 'assistant', 
            content: errorMessage,
            isError: true
          }]);
        }
      } else {
        // Handle conversational queries with Gemini AI
        if (apiError) {
          setMessages(prev => [...prev, { 
            type: 'assistant', 
            content: `🔧 ${apiError}`,
            isError: true
          }]);
        } else {
          const conversationalResponse = await generateConversationalResponse(currentInput);
          setMessages(prev => [...prev, { 
            type: 'assistant', 
            content: conversationalResponse,
            isConversation: true
          }]);
        }
      }
    } catch (error) {
      console.error('Error processing request:', error);
      setMessages(prev => [...prev, { 
        type: 'assistant', 
        content: `🔧 Sorry, something went wrong. Please try again.`,
        isError: true
      }]);
    } finally {
      setIsLoading(false);
    }
  };

  // Enhanced table rendering with better styling
  const renderResults = (results) => {
    if (!results || !results.length) return null;
    
    const headers = Object.keys(results[0]);
    
    return (
      <div className="report-data-container">
        <div className="data-summary">
          <span className="data-count">📊 {results.length} record(s) found</span>
        </div>
        <div className="enhanced-table-container">
          <table className="enhanced-results-table">
            <thead>
              <tr>
                {headers.map((header, index) => (
                  <th key={index}>
                    {header.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {results.slice(0, 50).map((row, rowIndex) => (
                <tr key={rowIndex} className={rowIndex % 2 === 0 ? 'even-row' : 'odd-row'}>
                  {headers.map((header, cellIndex) => (
                    <td key={cellIndex}>
                      {typeof row[header] === 'number' ? 
                        row[header].toLocaleString() : 
                        row[header] || 'N/A'
                      }
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {results.length > 50 && (
            <div className="table-pagination">
              <span>📄 Showing first 50 of {results.length} records</span>
            </div>
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="chatbot-container">
      <div className="chatbot-header">
        <h2>🤖 Smart Analytics Assistant</h2>
        <div className="ai-badge">
          Powered by Gemini AI
          {apiError && <span className="error-indicator">⚠️</span>}
        </div>
      </div>
      
      <div className="messages-container">
        {messages.map((message, index) => (
          <div 
            key={index} 
            className={`message ${message.type} ${message.isError ? 'error' : ''} ${message.isReport ? 'report-message' : ''} ${message.isConversation ? 'conversation-message' : ''}`}
          >
            <div className="message-content">
              {message.content && (
                <div className={message.isReport ? "report-summary" : "message-text"}>
                  {message.content.split('\n').map((line, lineIndex) => (
                    <div key={lineIndex} className="content-line">
                      {line.includes('**') ? (
                        <span dangerouslySetInnerHTML={{
                          __html: line.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
                        }} />
                      ) : (
                        line
                      )}
                    </div>
                  ))}
                </div>
              )}
              
              {message.results && renderResults(message.results)}
            </div>
          </div>
        ))}
        
        {isLoading && (
          <div className="message assistant loading">
            <div className="loading-indicator">
              <span></span><span></span><span></span>
            </div>
            <div className="loading-text">🧠 Thinking with AI...</div>
          </div>
        )}
        
        <div ref={messagesEndRef} />
      </div>
      
      <form onSubmit={handleSubmit} className="chatbot-input-form">
        <input
          ref={inputRef}
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder={apiError ? "Data queries only (AI chat unavailable)" : "Ask about data, tell me a joke, or let's chat..."}
          disabled={isLoading}
          className="chatbot-input"
        />
        <button 
          type="submit" 
          disabled={isLoading || !input.trim()}
          className="chatbot-submit-btn"
        >
          {isLoading ? '⏳' : '💬'}
        </button>
      </form>
    </div>
  );
};

export default Chatbot;
