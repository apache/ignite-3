import React, { useEffect, useRef, useState } from 'react';
import * as rr from './railroad.js';
import 'railroad-diagrams/railroad-diagrams.css';

interface RailroadDiagramProps {
  children: string;
}

export default function RailroadDiagram({ children }: RailroadDiagramProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);
  const modalContainerRef = useRef<HTMLDivElement>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);

  const renderDiagram = (targetRef: React.RefObject<HTMLDivElement>) => {
    if (!targetRef.current) return;

    try {
      // The custom railroad library supports href, title, and cls options on NonTerminal and Terminal
      // Create function wrappers that can be called without 'new'
      const Diagram = (...args: any[]) => new rr.Diagram(...args);
      const Terminal = (...args: any[]) => new rr.Terminal(...args);
      const NonTerminal = (...args: any[]) => new rr.NonTerminal(...args);
      const Optional = (...args: any[]) => new rr.Optional(...args);
      const Sequence = (...args: any[]) => new rr.Sequence(...args);
      const Choice = (...args: any[]) => new rr.Choice(...args);
      const Skip = (...args: any[]) => new rr.Skip(...args);
      const Comment = (...args: any[]) => new rr.Comment(...args);
      const OneOrMore = (...args: any[]) => new rr.OneOrMore(...args);
      const ZeroOrMore = (...args: any[]) => new rr.ZeroOrMore(...args);

      // Strip Start() and End() calls since Diagram() adds them automatically
      // Also replace ComplexDiagram with Diagram
      const cleanedCode = children
        .replace(/ComplexDiagram\(/g, 'Diagram(')
        .replace(/Start\([^)]*\),?\s*/g, '')
        .replace(/,?\s*End\([^)]*\)/g, '')
        .trim(); // CRITICAL: Remove leading/trailing whitespace to avoid 'return\nDiagram()' syntax error

      // Create function that executes the diagram code
      try {
        const diagramFunc = new Function(
          'Diagram', 'Terminal', 'NonTerminal', 'Optional', 'Sequence', 'Choice', 'Skip', 'Comment', 'OneOrMore', 'ZeroOrMore',
          `'use strict'; return ${cleanedCode};`
        );

        // Execute and get the diagram object
        const diagram = diagramFunc(Diagram, Terminal, NonTerminal, Optional, Sequence, Choice, Skip, Comment, OneOrMore, ZeroOrMore);

        if (!diagram || typeof diagram.toSVG !== 'function') {
          throw new Error('Diagram function did not return a valid diagram object');
        }

        // Convert to SVG and inject into container
        targetRef.current.innerHTML = '';
        const svgElement = diagram.toSVG();

        // Process links - relative paths like ./grammar-reference/#anchor
        // work correctly as-is since pages are in the same directory
        const links = svgElement.querySelectorAll('a[*|href]');
        links.forEach((link) => {
          const href = link.getAttributeNS('http://www.w3.org/1999/xlink', 'href');
          if (href) {
            // Convert xlink:href to standard href for Docusaurus router compatibility
            link.setAttribute('href', href);
          }
        });

        targetRef.current.appendChild(svgElement);
      } catch (innerError) {
        console.error('Error in diagram generation:', innerError);
        throw innerError;
      }
    } catch (error) {
      console.error('Error rendering railroad diagram:', error);
      targetRef.current.innerHTML = '<p style="color: red;">Error rendering diagram. Check console for details.</p>';
    }
  };

  useEffect(() => {
    renderDiagram(containerRef);
  }, [children]);

  useEffect(() => {
    if (isModalOpen) {
      renderDiagram(modalContainerRef);

      // Handle ESC key
      const handleEsc = (e: KeyboardEvent) => {
        if (e.key === 'Escape') {
          setIsModalOpen(false);
        }
      };
      document.addEventListener('keydown', handleEsc);

      // Prevent body scroll when modal is open
      document.body.style.overflow = 'hidden';

      return () => {
        document.removeEventListener('keydown', handleEsc);
        document.body.style.overflow = '';
      };
    }
  }, [isModalOpen, children]);

  const handleOverlayClick = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) {
      setIsModalOpen(false);
    }
  };

  return (
    <>
      <div className="railroad-diagram-wrapper">
        <button
          className="railroad-expand-button"
          onClick={() => setIsModalOpen(true)}
          aria-label="Expand diagram"
          title="View full size"
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
            <path d="M1.5 1a.5.5 0 0 0-.5.5v4a.5.5 0 0 1-1 0v-4A1.5 1.5 0 0 1 1.5 0h4a.5.5 0 0 1 0 1h-4zM10 .5a.5.5 0 0 1 .5-.5h4A1.5 1.5 0 0 1 16 1.5v4a.5.5 0 0 1-1 0v-4a.5.5 0 0 0-.5-.5h-4a.5.5 0 0 1-.5-.5zM.5 10a.5.5 0 0 1 .5.5v4a.5.5 0 0 0 .5.5h4a.5.5 0 0 1 0 1h-4A1.5 1.5 0 0 1 0 14.5v-4a.5.5 0 0 1 .5-.5zm15 0a.5.5 0 0 1 .5.5v4a1.5 1.5 0 0 1-1.5 1.5h-4a.5.5 0 0 1 0-1h4a.5.5 0 0 0 .5-.5v-4a.5.5 0 0 1 .5-.5z"/>
          </svg>
        </button>
        <div ref={containerRef} className="railroad-diagram-container" />
      </div>

      {isModalOpen && (
        <div className="railroad-modal-overlay" onClick={handleOverlayClick}>
          <div className="railroad-modal-content">
            <button
              className="railroad-modal-close"
              onClick={() => setIsModalOpen(false)}
              aria-label="Close"
              title="Close (ESC)"
            >
              ×
            </button>
            <div ref={modalContainerRef} className="railroad-diagram-container railroad-diagram-modal" />
          </div>
        </div>
      )}
    </>
  );
}
